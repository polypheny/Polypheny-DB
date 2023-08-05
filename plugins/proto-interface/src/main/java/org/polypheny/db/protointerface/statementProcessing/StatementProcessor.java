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

package org.polypheny.db.protointerface.statementProcessing;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.protointerface.PIServiceException;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.relational.RelationalMetaRetriever;
import org.polypheny.db.protointerface.statements.PIPreparedStatement;
import org.polypheny.db.protointerface.statements.PIStatement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.Pair;

public class StatementProcessor {

    private static final Map<QueryLanguage, StatementExecutor> EXECUTORS =
            ImmutableMap.<QueryLanguage, StatementExecutor>builder()
                    .put( QueryLanguage.from( "sql" ), new SqlExecutor() )
                    .put( QueryLanguage.from( "mql" ), new MqlExecutor() )
                    .build();
    private static final Map<NamespaceType, ResultRetriever> RESULT_RETRIEVERS =
            ImmutableMap.<NamespaceType, ResultRetriever>builder()
                    .put( NamespaceType.RELATIONAL, new RelationalResultRetriever() )
                    .put( NamespaceType.DOCUMENT, new DocumentResultRetriever() )
                    .build();


    public static void execute( PIStatement piStatement ) {
        StatementExecutor statementExecutor = EXECUTORS.get( piStatement.getLanguage() );
        if ( statementExecutor == null ) {
            throw new PIServiceException( "No executor registered for language " + piStatement.getLanguage(),
                    "I9005",
                    9005
            );
        }

    }


    public static StatementResult getResult( PIStatement piStatement ) throws Exception {
        ResultRetriever resultRetriever = RESULT_RETRIEVERS.get( piStatement.getLanguage().getNamespaceType() );
        if ( resultRetriever == null ) {
            throw new PIServiceException( "No result retriever registered for namespace type "
                    + piStatement.getLanguage().getNamespaceType(),
                    "I9004",
                    9004
            );
        }
        return resultRetriever.getResult( piStatement );
    }


    public static Frame fetch( PIStatement piStatement ) {
        ResultRetriever resultRetriever = RESULT_RETRIEVERS.get( piStatement.getLanguage().getNamespaceType() );
        if ( resultRetriever == null ) {
            throw new PIServiceException( "No result retriever registered for namespace type "
                    + piStatement.getLanguage().getNamespaceType(),
                    "I9004",
                    9004
            );
        }
        return resultRetriever.fetch( piStatement );
    }


    public static Frame fetchGraphFrame( PIStatement piStatement ) {
        throw new RuntimeException( "Feature not implemented" );
        /*
        Statement statement = piStatement.getStatement();
        PolyImplementation<PolyValue> implementation = piStatement.getImplementation();
        //TODO TH:  Whats the actual type here?
        List<PolyValue[]> data = implementation.getArrayRows( statement, true );
        return ProtoUtils.buildGraphFrame();
        */
    }


    public static void prepare( PIPreparedStatement piStatement ) {
        Transaction transaction = piStatement.getClient().getCurrentOrCreateNewTransaction();
        String query = piStatement.getQuery();
        QueryLanguage queryLanguage = piStatement.getLanguage();

        Processor queryProcessor = transaction.getProcessor( queryLanguage );
        Node parsed = queryProcessor.parse( query ).get( 0 );
        // It is important not to add default values for missing fields in insert statements. If we did this, the
        // JDBC driver would expect more parameter fields than there actually are in the query.
        Pair<Node, AlgDataType> validated = queryProcessor.validate( transaction, parsed, false );
        AlgDataType parameterRowType = queryProcessor.getParameterRowType( validated.left );
        piStatement.setParameterMetas( RelationalMetaRetriever.retrieveParameterMetas( parameterRowType ) );
    }

}
