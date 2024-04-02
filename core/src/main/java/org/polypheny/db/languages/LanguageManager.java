/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.languages;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.Pair;

@Slf4j
public class LanguageManager {

    private final PropertyChangeSupport listeners = new PropertyChangeSupport( this );

    @Getter
    private static final LanguageManager INSTANCE = new LanguageManager();

    private static final List<QueryLanguage> REGISTER = new ArrayList<>();


    private LanguageManager() {

    }


    public static List<QueryLanguage> getLanguages() {
        return REGISTER;
    }


    public void addObserver( PropertyChangeListener listener ) {
        listeners.addPropertyChangeListener( listener );
    }


    public void removeObserver( PropertyChangeListener listener ) {
        listeners.removePropertyChangeListener( listener );
    }


    public void addQueryLanguage( QueryLanguage language ) {
        REGISTER.add( language );
        listeners.firePropertyChange( "language", null, language );
    }


    public static void removeQueryLanguage( String name ) {
        REGISTER.remove( QueryLanguage.from( name ) );
    }


    public List<ImplementationContext> anyPrepareQuery( QueryContext context, Transaction transaction ) {
        return anyPrepareQuery( context, transaction.createStatement() );
    }


    // This method is still called from the Avatica interface and leaves the statement management to the caller.
    // This should be refactored to use the new method only transmitting the transaction as soon as the
    // new prism interface is enabled
    public List<ImplementationContext> anyPrepareQuery( QueryContext context, Statement statement ) {
        Transaction transaction = statement.getTransaction();
        if ( transaction.isAnalyze() ) {
            context.getInformationTarget().accept( statement.getTransaction().getQueryAnalyzer() );
        }

        if ( transaction.isAnalyze() ) {
            statement.getOverviewDuration().start( "Parsing" );
        }
        List<ParsedQueryContext> parsedQueries;

        try {
            // handle empty query
            if ( context.getQuery().trim().isEmpty() ) {
                throw new GenericRuntimeException( String.format( "%s query is empty", context.getLanguage().serializedName() ) );
            }

            parsedQueries = context.getLanguage().splitter().apply( context );
        } catch ( Throwable e ) {
            log.warn( "Error on preparing query: " + e.getMessage() );
            if ( transaction.isAnalyze() ) {
                transaction.getQueryAnalyzer().attachStacktrace( e );
            }
            cancelTransaction( transaction );
            return List.of( ImplementationContext.ofError( e, ParsedQueryContext.fromQuery( context.getQuery(), null, context ), statement ) );
        }

        if ( transaction.isAnalyze() ) {
            statement.getOverviewDuration().stop( "Parsing" );
        }

        Processor processor = context.getLanguage().processorSupplier().get();
        List<ImplementationContext> implementationContexts = new ArrayList<>();
        boolean previousDdl = false;
        int i = 0;
        for ( ParsedQueryContext parsed : parsedQueries ) {
            if ( i != 0 ) {
                statement = transaction.createStatement();
            }
            try {
                // test if parsing was successful
                if ( parsed.getQueryNode().isEmpty() ) {
                    Exception e = new GenericRuntimeException( "Error during parsing of query \"" + context.getQuery() + "\"" );
                    return handleParseException( statement, parsed, transaction, e, implementationContexts );
                }

                PolyImplementation implementation;
                // routing to appropriate handler
                if ( parsed.getQueryNode().get().isDdl() ) {
                    // check if statement is executable
                    if ( !(parsed.getQueryNode().get() instanceof ExecutableStatement) ) {
                        return handleParseException(
                                statement,
                                parsed,
                                transaction,
                                new GenericRuntimeException( "DDL statement is not executable" ),
                                implementationContexts );
                    }
                    // as long as we directly commit the transaction, we cannot reuse the same transaction
                    if ( previousDdl && !transaction.isActive() ) {
                        transaction = parsed.getTransactionManager().startTransaction( transaction.getUser().id, transaction.getDefaultNamespace().id, transaction.isAnalyze(), transaction.getOrigin() );
                        statement = transaction.createStatement();
                        parsed.addTransaction( transaction );
                    }

                    implementation = processor.prepareDdl( statement, (ExecutableStatement) parsed.getQueryNode().get(), parsed );
                    previousDdl = true;
                } else {
                    // as long as we directly commit the transaction, we cannot reuse the same transaction
                    if ( previousDdl && !transaction.isActive() ) {
                        transaction = parsed.getTransactionManager().startTransaction( transaction.getUser().id, transaction.getDefaultNamespace().id, transaction.isAnalyze(), transaction.getOrigin() );
                        statement = transaction.createStatement();
                        parsed.addTransaction( transaction );
                    }
                    previousDdl = false;
                    if ( context.getLanguage().validatorSupplier() != null ) {
                        if ( transaction.isAnalyze() ) {
                            statement.getOverviewDuration().start( "Validation" );
                        }
                        Pair<Node, AlgDataType> validated = processor.validate(
                                transaction,
                                parsed.getQueryNode().get(),
                                RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() );
                        parsed = ParsedQueryContext.fromQuery( parsed.getQuery(), validated.left, context );
                        if ( transaction.isAnalyze() ) {
                            statement.getOverviewDuration().stop( "Validation" );
                        }
                    }

                    if ( transaction.isAnalyze() ) {
                        statement.getOverviewDuration().start( "Translation" );
                    }

                    AlgRoot root = processor.translate( statement, parsed );

                    if ( transaction.isAnalyze() ) {
                        statement.getOverviewDuration().stop( "Translation" );
                    }
                    implementation = statement.getQueryProcessor().prepareQuery( root, true );
                }
                implementationContexts.add( new ImplementationContext( implementation, parsed, statement, null ) );

            } catch ( Throwable e ) {
                log.warn( "Caught exception: ", e );
                if ( transaction.isAnalyze() ) {
                    transaction.getQueryAnalyzer().attachStacktrace( e );
                }
                cancelTransaction( transaction );
                implementationContexts.add( ImplementationContext.ofError( e, parsed, statement ) );
                return implementationContexts;
            }
            i++;
        }
        return implementationContexts;
    }


    @NotNull
    private static List<ImplementationContext> handleParseException( Statement statement, ParsedQueryContext parsed, Transaction transaction, Exception e, List<ImplementationContext> implementationContexts ) {
        if ( transaction.isAnalyze() ) {
            transaction.getQueryAnalyzer().attachStacktrace( e );
        }
        implementationContexts.add( ImplementationContext.ofError( e, parsed, statement ) );
        return implementationContexts;
    }


    private static void cancelTransaction( @Nullable Transaction transaction ) {
        if ( transaction != null && transaction.isActive() ) {
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                // Ignore
                log.warn( "Error during rollback: " + ex.getMessage() );
            }
        }
    }


    public List<ExecutedContext> anyQuery( QueryContext context ) {
        List<ImplementationContext> prepared = anyPrepareQuery( context, context.getTransactions().get( context.getTransactions().size() - 1 ) );

        List<ExecutedContext> executedContexts = new ArrayList<>();

        for ( ImplementationContext implementation : prepared ) {
            try {
                if ( implementation.getException().isPresent() ) {
                    throw implementation.getException().get();
                }
                executedContexts.add( implementation.execute( implementation.getStatement() ) );
            } catch ( Throwable e ) {
                Transaction transaction = implementation.getStatement().getTransaction();
                if ( transaction.isAnalyze() && implementation.getException().isEmpty() ) {
                    transaction.getQueryAnalyzer().attachStacktrace( e );
                }
                cancelTransaction( transaction );

                executedContexts.add( ExecutedContext.ofError( e, implementation, null ) );
                return executedContexts;
            }
        }

        return executedContexts;
    }


    public static List<ParsedQueryContext> toQueryNodes( QueryContext queries ) {
        Processor processor = queries.getLanguage().processorSupplier().get();
        List<String> splitQueries = processor.splitStatements( queries.getQuery() );

        return splitQueries.stream().flatMap( q -> processor.parse( q ).stream().map( single -> Pair.of( single, q ) ) )
                .map( p -> ParsedQueryContext.fromQuery( p.right, p.left, queries ) )
                .toList();
    }

}
