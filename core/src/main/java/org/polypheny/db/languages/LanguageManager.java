/*
 * Copyright 2019-2023 The Polypheny Project
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
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.Pair;

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


    public List<ImplementationContext> anyPrepareQuery( QueryContext context, Statement statement ) {
        //Statement statement = transaction.createStatement();
        Transaction transaction = statement.getTransaction();

        if ( transaction.isAnalyze() ) {
            context.getInformationTarget().accept( transaction.getQueryAnalyzer() );
        }

        if ( transaction.isAnalyze() ) {
            statement.getOverviewDuration().start( "Parsing" );
        }
        List<ParsedQueryContext> parsedQueries = context.getLanguage().getSplitter().apply( context );
        if ( transaction.isAnalyze() ) {
            statement.getOverviewDuration().stop( "Parsing" );
        }

        Processor processor = context.getLanguage().getProcessorSupplier().get();
        List<ImplementationContext> implementationContexts = new ArrayList<>();
        for ( ParsedQueryContext parsed : parsedQueries ) {
            try {

                PolyImplementation implementation;
                if ( parsed.getQueryNode().isDdl() ) {
                    implementation = processor.prepareDdl( statement, parsed );
                } else {

                    if ( context.getLanguage().getValidatorSupplier() != null ) {
                        if ( transaction.isAnalyze() ) {
                            statement.getOverviewDuration().start( "Validation" );
                        }
                        Pair<Node, AlgDataType> validated = processor.validate(
                                transaction,
                                parsed.getQueryNode(),
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

            } catch ( Exception e ) {
                if ( transaction.isAnalyze() ) {
                    transaction.getQueryAnalyzer().attachStacktrace( e );
                }
                implementationContexts.add( ImplementationContext.ofError( e, parsed, statement ) );
                return implementationContexts;
            }
        }
        return implementationContexts;
    }


    public List<ExecutedContext> anyQuery( QueryContext context, Statement statement ) {

        List<ImplementationContext> prepared = anyPrepareQuery( context, statement );
        Transaction transaction = statement.getTransaction();

        List<ExecutedContext> executedContexts = new ArrayList<>();

        for ( ImplementationContext implementation : prepared ) {
            try {
                if ( implementation.getException().isPresent() ) {
                    throw implementation.getException().get();
                }
                if ( context.isAnalysed() ) {
                    implementation.getStatement().getOverviewDuration().start( "Execution" );
                }
                executedContexts.add( implementation.execute( implementation.getStatement() ) );
                if ( context.isAnalysed() ) {
                    implementation.getStatement().getOverviewDuration().stop( "Execution" );
                }
            } catch ( Exception e ) {
                if ( transaction.isAnalyze() ) {
                    transaction.getQueryAnalyzer().attachStacktrace( e );
                }
                executedContexts.add( ExecutedContext.ofError( e, implementation ) );
                return executedContexts;
            }
        }

        return executedContexts;
    }


    public static List<ParsedQueryContext> toQueryNodes( QueryContext queries ) {
        Processor cypherProcessor = queries.getLanguage().getProcessorSupplier().get();
        List<? extends Node> statements = cypherProcessor.parse( queries.getQuery() );

        return Pair.zip( statements, List.of( queries.getQuery().split( ";" ) ) )
                .stream()
                .map( p -> ParsedQueryContext.fromQuery( p.right, p.left, queries ) )
                .collect( Collectors.toList() );
    }

}
