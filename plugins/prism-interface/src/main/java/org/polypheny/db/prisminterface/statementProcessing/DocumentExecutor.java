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
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.PIServiceException;
import org.polypheny.db.prisminterface.statements.PIStatement;
import org.polypheny.db.prisminterface.utils.PrismUtils;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.prism.Frame;
import org.polypheny.prism.StatementResult;

public class DocumentExecutor extends Executor {

    private static final DataModel namespaceType = DataModel.DOCUMENT;


    @Override
    DataModel getDataModel() {
        return namespaceType;
    }


    @Override
    StatementResult executeAndGetResult( PIStatement piStatement ) {
        if ( hasInvalidNamespaceType( piStatement ) ) {
            throw new PIServiceException( "The results of type "
                    + piStatement.getLanguage().dataModel()
                    + "returned by this statement can't be retrieved by a document retriever.",
                    "I9000",
                    9000
            );
        }
        PolyImplementation implementation = piStatement.getImplementation();
        if ( implementation == null ) {
            throw new PIServiceException( "Can't retrieve results form an unexecuted statement.",
                    "I9002",
                    9002
            );
        }
        PIClient client = piStatement.getClient();
        StatementResult.Builder resultBuilder = StatementResult.newBuilder();
        if ( implementation.isDDL() ) {
            resultBuilder.setScalar( 1 );
            return resultBuilder.build();
        }
        throw new PIServiceException( "Can't execute a non DDL or non DML statement using this method..",
                "I9003",
                9002
        );
    }


    @Override
    StatementResult executeAndGetResult( PIStatement piStatement, int fetchSize ) {
        if ( hasInvalidNamespaceType( piStatement ) ) {
            throw new PIServiceException( "The results of type "
                    + piStatement.getLanguage().dataModel()
                    + "returned by this statement can't be retrieved by a document retriever.",
                    "I9000",
                    9000
            );
        }
        PolyImplementation implementation = piStatement.getImplementation();
        if ( implementation == null ) {
            throw new PIServiceException( "Can't retrieve results form an unexecuted statement.",
                    "I9002",
                    9002
            );
        }
        PIClient client = piStatement.getClient();
        StatementResult.Builder resultBuilder = StatementResult.newBuilder();
        if ( implementation.isDDL() ) {
            resultBuilder.setScalar( 1 );
            return resultBuilder.build();
        }
        piStatement.setIterator( implementation.execute( piStatement.getStatement(), fetchSize ) );
        Frame frame = fetch( piStatement, fetchSize );
        resultBuilder.setFrame( frame );
        if ( frame.getIsLast() ) {
            //TODO TH: special handling for result set updates. Do we need to wait with committing until all changes have been done?
            client.commitCurrentTransactionIfAuto();
        }
        return resultBuilder.build();
    }


    @Override
    Frame fetch( PIStatement piStatement, int fetchSize ) {
        if ( hasInvalidNamespaceType( piStatement ) ) {
            throw new PIServiceException( "The results of type "
                    + piStatement.getLanguage().dataModel()
                    + "returned by this statement can't be retrieved by a document retriever.",
                    "I9000",
                    9000
            );
        }
        StopWatch executionStopWatch = piStatement.getExecutionStopWatch();
        Statement statement = piStatement.getStatement();
        if ( statement == null ) {
            throw new PIServiceException( "Statement is not linked to a polypheny statement",
                    "I9001",
                    9001
            );
        }
        PolyImplementation implementation = piStatement.getImplementation();
        if ( implementation == null ) {
            throw new PIServiceException( "Can't fetch from an unexecuted statement.",
                    "I9002",
                    9002
            );
        }
        ResultIterator iterator = piStatement.getIterator();
        if ( iterator == null ) {
            throw new PIServiceException( "Can't fetch from an unexecuted statement.",
                    "I9002",
                    9002
            );
        }
        startOrResumeStopwatch( executionStopWatch );
        List<PolyValue> data = iterator.getNextBatch( fetchSize ).stream().map( p -> p.get( 0 ) ).toList();
        executionStopWatch.stop();
        return PrismUtils.buildDocumentFrame( !iterator.hasMoreRows(), data );
    }

}
