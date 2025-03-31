/*
 * Copyright 2019-2025 The Polypheny Project
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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.processing.QueryContext.PhysicalQueryContext;
import org.polypheny.db.processing.QueryContext.TranslatedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.DeadlockException;
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
        return anyPrepareQuery( context, context.getStatement() != null ? context.getStatement() : transaction.createStatement() );
    }


    // This method is still called from the Avatica interface and leaves the statement management to the caller.
    // This should be refactored to use the new method only transmitting the transaction as soon as the
    // new prism interface is enabled
    public List<ImplementationContext> anyPrepareQuery( QueryContext context, Statement statement ) {
        Transaction transaction = statement.getTransaction();
        if ( transaction.isAnalyze() ) {
            context.getInformationTarget().accept( statement.getTransaction().getQueryAnalyzer() );
        }

        List<ParsedQueryContext> parsedQueries;

        if ( context instanceof ParsedQueryContext ) {
            parsedQueries = List.of( (ParsedQueryContext) context );
        } else {
            try {
                if ( transaction.isAnalyze() ) {
                    statement.getOverviewDuration().start( "Parsing" );
                }

                // handle empty query
                if ( context.getQuery().trim().isEmpty() ) {
                    throw new GenericRuntimeException( String.format( "%s query is empty", context.getLanguage().serializedName() ) );
                }

                parsedQueries = context.getLanguage().parser().apply( context );
            } catch ( Throwable e ) {
                if ( transaction.isAnalyze() ) {
                    transaction.getQueryAnalyzer().attachStacktrace( e );
                }
                cancelTransaction( transaction, String.format( "Error on preparing query: %s", e.getMessage() ) );
                context.removeTransaction( transaction );
                return List.of( ImplementationContext.ofError( e, ParsedQueryContext.fromQuery( context.getQuery(), null, context ), statement ) );
            }

            if ( transaction.isAnalyze() ) {
                statement.getOverviewDuration().stop( "Parsing" );
            }

        }

        if ( context instanceof TranslatedQueryContext ) {
            return implementTranslatedQuery( statement, transaction, (TranslatedQueryContext) context );
        }

        Processor processor = context.getLanguage().processorSupplier().get();
        List<ImplementationContext> implementationContexts = new ArrayList<>();
        boolean previousDdl = false;
        int i = 0;
        String changedNamespace = null;
        for ( ParsedQueryContext parsed : parsedQueries ) {
            if ( i != 0 ) {
                // as long as we directly commit the transaction, we cannot reuse the same transaction
                if ( previousDdl && !transaction.isActive() ) {
                    transaction = parsed.getTransactionManager().startTransaction( transaction.getUser().id, transaction.getDefaultNamespace().id, transaction.isAnalyze(), transaction.getOrigin() );
                    parsed.addTransaction( transaction );
                }
                statement = transaction.createStatement();
            }
            if ( changedNamespace != null ) {
                parsed = parsed.toBuilder().namespaceId( Catalog.snapshot().getNamespace( changedNamespace ).map( n -> n.id ).orElse( parsed.getNamespaceId() ) ).build();
            }

            try {
                // test if parsing was successful
                if ( parsed.getQueryNode().isEmpty() ) {
                    Exception e = new GenericRuntimeException( "Error during parsing of query \"%s\"".formatted( context.getQuery() ) );
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

                    implementation = processor.prepareDdl( statement, (ExecutableStatement) parsed.getQueryNode().get(), parsed );
                    previousDdl = true;
                } else {
                    previousDdl = false;
                    if ( parsed.getLanguage().validatorSupplier() != null ) {
                        if ( transaction.isAnalyze() ) {
                            statement.getOverviewDuration().start( "Validation" );
                        }
                        Pair<Node, AlgDataType> validated = processor.validate(
                                transaction,
                                parsed.getQueryNode().get(),
                                RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() );
                        parsed = ParsedQueryContext.fromQuery( parsed.getQuery(), validated.left, parsed );
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

                    if ( !statement.getTransaction().isActive() ) {
                        log.warn( "Transaction is not active" );
                    }
                    implementation = statement.getQueryProcessor().prepareQuery( root, true );
                }
                // queries are able to switch the context of the following queries
                changedNamespace = parsed.getQueryNode().orElseThrow().switchesNamespace().orElse( changedNamespace );

                implementationContexts.add( new ImplementationContext( implementation, parsed, statement, null ) );

            } catch ( Throwable e ) {
                if ( !(e instanceof DeadlockException) ) {
                    // we only log unexpected cases with stacktrace
                    log.warn( "Caught exception: ", e );
                }

                if ( transaction.isAnalyze() ) {
                    transaction.getQueryAnalyzer().attachStacktrace( e );
                }
                cancelTransaction( transaction, e.getMessage() );
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


    private static void cancelTransaction( @Nullable Transaction transaction, @Nullable String reason ) {
        if ( transaction != null && transaction.isActive() ) {
            transaction.rollback( reason );
        }
    }


    public List<ExecutedContext> anyQuery( QueryContext context ) {
        List<ImplementationContext> prepared;
        if ( context instanceof TranslatedQueryContext ) {
            prepared = anyPrepareQuery( context, context.getStatement() );
        } else {
            prepared = anyPrepareQuery( context, context.getTransactions().get( context.getTransactions().size() - 1 ) );
        }
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
                cancelTransaction( transaction, e.getMessage() );

                executedContexts.add( ExecutedContext.ofError( e, implementation, null ) );
                return executedContexts;
            }
        }

        return executedContexts;
    }


    private List<ImplementationContext> implementTranslatedQuery( Statement statement, Transaction transaction, TranslatedQueryContext translated ) {
        try {
            PolyImplementation implementation;

            if ( translated instanceof PhysicalQueryContext physical ) {
                for ( int i = 0; i < physical.getDynamicValues().size(); i++ ) {
                    PolyValue v = physical.getDynamicValues().get( i );
                    AlgDataType type = physical.getDynamicTypes().get( i );
                    statement.getDataContext().addParameterValues( i, type, List.of( v ) );
                }
                implementation = statement.getQueryProcessor().prepareQuery( physical.getRoot(), translated.isRouted(), true, true );
            } else {
                implementation = statement.getQueryProcessor().prepareQuery( translated.getRoot(), translated.isRouted(), true );
            }

            return List.of( new ImplementationContext( implementation, translated, statement, null ) );
        } catch ( Throwable e ) {
            if ( transaction.isAnalyze() ) {
                transaction.getQueryAnalyzer().attachStacktrace( e );
            }
            if ( !(e instanceof DeadlockException) ) {
                // we only log unexpected cases with stacktrace
                log.warn( "Caught exception: ", e );
            }

            cancelTransaction( transaction, String.format( "Caught %s exception: %s", e.getClass().getSimpleName(), e.getMessage() ) );
            return List.of( (ImplementationContext.ofError( e, translated, statement )) );
        }
    }


    public static List<ParsedQueryContext> toQueryNodes( QueryContext queries ) {
        Processor processor = queries.getLanguage().processorSupplier().get();
        List<String> splitQueries = processor.splitStatements( queries.getQuery() );

        return splitQueries.stream().flatMap( q -> processor.parse( q ).stream().map( single -> Pair.of( single, q ) ) )
                .map( p -> ParsedQueryContext.fromQuery( p.right, p.left, queries ) )
                .toList();
    }


    public static List<ParsedQueryContext> toUnsplitQueryNodes( QueryContext queries ) {
        Processor processor = queries.getLanguage().processorSupplier().get();
        List<String> splitQueries = List.of( queries.getQuery() );

        return splitQueries.stream().flatMap( q -> processor.parse( q ).stream().map( single -> Pair.of( single, q ) ) )
                .map( p -> ParsedQueryContext.fromQuery( p.right, p.left, queries ) )
                .toList();
    }

}
