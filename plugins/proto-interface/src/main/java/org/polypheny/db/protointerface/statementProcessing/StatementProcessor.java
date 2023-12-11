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
import org.polypheny.db.catalog.logistic.DataModel;
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

    private static final Map<QueryLanguage, StatementImplementer> EXECUTORS =
            ImmutableMap.<QueryLanguage, StatementImplementer>builder()
                    .put( QueryLanguage.from( "sql" ), new SqlImplementer() )
                    .put( QueryLanguage.from( "mongo" ), new MongoImplementer() )
                    .build();
    private static final Map<DataModel, Executor> RESULT_RETRIEVERS =
            ImmutableMap.<DataModel, Executor>builder()
                    .put( DataModel.RELATIONAL, new RelationalExecutor() )
                    .put( DataModel.DOCUMENT, new DocumentExecutor() )
                    .build();


    public static void implement(PIStatement piStatement ) {
        StatementImplementer statementImplementer = EXECUTORS.get( piStatement.getLanguage() );
        if ( statementImplementer == null ) {
            throw new PIServiceException( "No executor registered for language " + piStatement.getLanguage(),
                    "I9005",
                    9005
            );
        }
        statementImplementer.implement( piStatement );
    }

    public static StatementResult executeAndGetResult(PIStatement piStatement) throws Exception {
        Executor executor = RESULT_RETRIEVERS.get( piStatement.getLanguage().getDataModel() );
        if ( executor == null ) {
            throw new PIServiceException( "No result retriever registered for namespace type "
                    + piStatement.getLanguage().getDataModel(),
                    "I9004",
                    9004
            );
        }
        return executor.executeAndGetResult( piStatement);
    }

    public static StatementResult executeAndGetResult(PIStatement piStatement, int fetchSize ) throws Exception {
        Executor executor = RESULT_RETRIEVERS.get( piStatement.getLanguage().getDataModel() );
        if ( executor == null ) {
            throw new PIServiceException( "No result retriever registered for namespace type "
                    + piStatement.getLanguage().getDataModel(),
                    "I9004",
                    9004
            );
        }
        return executor.executeAndGetResult( piStatement, fetchSize );
    }


    public static Frame fetch( PIStatement piStatement, int fetchSize ) {
        Executor executor = RESULT_RETRIEVERS.get( piStatement.getLanguage().getDataModel() );
        if ( executor == null ) {
            throw new PIServiceException( "No result retriever registered for namespace type "
                    + piStatement.getLanguage().getDataModel(),
                    "I9004",
                    9004
            );
        }
        return executor.fetch( piStatement, fetchSize );
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
