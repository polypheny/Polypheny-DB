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

package org.polypheny.db.prisminterface.statementProcessing;

import java.util.List;
import lombok.SneakyThrows;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.PIServiceException;
import org.polypheny.db.prisminterface.relational.RelationalMetaRetriever;
import org.polypheny.db.prisminterface.statements.PIPreparedStatement;
import org.polypheny.db.prisminterface.statements.PIStatement;
import org.polypheny.db.processing.ImplementationContext;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;
import org.polypheny.prism.Frame;
import org.polypheny.prism.StatementResult;

public class StatementProcessor {

    private static final String ORIGIN = "prism-interface";


    public static Node parseGraphStatement( PIStatement piStatement ) {
        List<? extends Node> statements = piStatement.getLanguage().processorSupplier().get().parse( piStatement.getQuery() );
        if ( statements.size() != 1 ) {
            throw new PIServiceException( "Prism requires exactly one Cypher statement per request." );
        }
        return statements.get( 0 );
    }


    /**
     * Checks every parsed operation before using a transaction shared with an open result.
     */
    public static @Nullable List<ParsedQueryContext> parseWithOpenAutocommitResult( PIStatement piStatement ) {
        PIClient client = piStatement.getClient();
        if ( !client.isAutoCommit() || !client.getStatementManager().hasOpenResult( false ) ) {
            return null;
        }

        QueryContext context = queryContext( piStatement, client.getTransactionManager() );
        // The registered MQL parser may also generate DDL for a missing collection.
        List<ParsedQueryContext> parsed = piStatement.getLanguage().parser().apply( context );
        if ( parsed.isEmpty() ) {
            throw new PIServiceException( "The " + piStatement.getLanguage().serializedName() + " query is empty" );
        }
        if ( parsed.stream().anyMatch( query -> query.getQueryNode().filter( StatementProcessor::isReadOnly ).isEmpty() ) ) {
            throw new PIServiceException( "Close or finish the open result before executing a write or command in autocommit mode" );
        }
        return parsed;
    }


    private static boolean isReadOnly( Node statement ) {
        return !statement.isDdl() && (statement.isA( Kind.QUERY ) || statement.getKind() == Kind.EXPLAIN);
    }


    public static void implement( PIStatement piStatement ) {
        implement( piStatement, null );
    }


    public static void implement( PIStatement piStatement, @Nullable Node parsedStatement ) {
        implement( piStatement, parsedStatement, parseWithOpenAutocommitResult( piStatement ) );
    }


    // @SneakyThrows is needed for rethrowing implementation.get( 0 ).getException() without
    // having to annotate everything with "throws Throwable" or wrapping it in another Exception
    @SneakyThrows
    public static void implement( PIStatement piStatement, @Nullable Node parsedStatement, @Nullable List<ParsedQueryContext> preParsedQueries ) {
        Statement statement = piStatement.getStatement();
        if ( statement == null ) {
            throw new PIServiceException( "Statement is not linked to a PolyphenyStatement",
                    "I9003",
                    9003
            );
        }
        QueryContext context = queryContext( piStatement, statement.getTransaction().getTransactionManager() );
        if ( parsedStatement != null ) {
            context = ParsedQueryContext.fromQuery( context.getQuery(), parsedStatement, context );
        }
        List<ImplementationContext> implementations = LanguageManager.getINSTANCE().anyPrepareQuery( context, statement, preParsedQueries );
        if ( implementations.stream().anyMatch( i -> i.getQuery().isAutoGenerated() ) ) {
            // we remove additional transaction which where opened for auto generation todo adjust if interface supports it
            implementations.stream()
                    .filter( i -> !i.getStatement().getTransaction().getXid().equals( piStatement.getStatement().getTransaction().getXid() ) )
                    .forEach( i -> i.getStatement().getTransaction().rollback( "Closing auto-generated transactions" ) );

            throw new GenericRuntimeException( "This query relies on an auto-generated subquery, which is not supported by this interface. Transaction was rolled backed." );
        }

        if ( implementations.get( 0 ).getImplementation() == null ) {
            throw implementations.get( 0 ).getException().orElseThrow();
        }
        piStatement.setImplementation( implementations.get( 0 ).getImplementation() );
    }


    private static QueryContext queryContext( PIStatement piStatement, TransactionManager transactionManager ) {
        return QueryContext.builder()
                .query( piStatement.getQuery() )
                .language( piStatement.getLanguage() )
                .namespaceId( piStatement.getNamespace().id )
                .transactionManager( transactionManager )
                .origin( ORIGIN )
                .build();
    }


    public static StatementResult executeAndGetResult( PIStatement piStatement ) {
        return ResultExecutor.executeCount( piStatement );
    }


    public static StatementResult executeAndGetResult( PIStatement piStatement, int fetchSize ) {
        return ResultExecutor.execute( piStatement, fetchSize );
    }


    public static Frame fetch( PIStatement piStatement, int fetchSize ) {
        return ResultExecutor.fetch( piStatement, fetchSize );
    }


    public static void prepare( PIPreparedStatement piStatement ) {
        QueryLanguage queryLanguage = piStatement.getLanguage();
        if ( queryLanguage.dataModel() == DataModel.GRAPH ) {
            throw new PIServiceException( "Prepared Cypher statements are not supported by Prism" );
        }
        Transaction transaction = piStatement.getClient().getOrCreateNewTransaction();
        String query = piStatement.getQuery();

        Processor queryProcessor = transaction.getProcessor( queryLanguage );
        Node parsed = queryProcessor.parse( query ).get( 0 );
        // It is important not to add default values for missing fields in insert statements. If we did this, the
        // JDBC driver would expect more parameter fields than there actually are in the query.
        Pair<Node, AlgDataType> validated = queryProcessor.validate( transaction, parsed, false );
        AlgDataType parameterRowType = queryProcessor.getParameterRowType( validated.left );
        piStatement.setParameterMetas( RelationalMetaRetriever.retrieveParameterMetas( parameterRowType ) );
        piStatement.setParameterPolyTypes( parameterRowType.getFields().stream().map( AlgDataTypeField::getType ).toList() );
    }

}
