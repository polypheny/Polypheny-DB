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
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.monitoring.events.MonitoringType;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.PIServiceException;
import org.polypheny.db.prisminterface.metaRetrieval.RelationalMetaRetriever;
import org.polypheny.db.prisminterface.statements.PIStatement;
import org.polypheny.db.prisminterface.utils.PrismUtils;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.Frame;
import org.polypheny.prism.StatementResult;

public class RelationalExecutor extends Executor {

    private static final DataModel namespaceType = DataModel.RELATIONAL;


    @Override
    DataModel getDataModel() {
        return namespaceType;
    }


    @Override
    StatementResult executeAndGetResult( PIStatement piStatement ) {
        throwOnIllegalState( piStatement );
        Statement statement = piStatement.getStatement();
        PolyImplementation implementation = piStatement.getImplementation();
        StatementResult.Builder resultBuilder = StatementResult.newBuilder();
        if ( implementation.isDDL() || Kind.DML.contains( implementation.getKind() ) ) {
            try ( ResultIterator iterator = implementation.execute( statement, -1 ) ) {
                resultBuilder.setScalar( PolyImplementation.getRowsChanged( statement, iterator.getIterator(), MonitoringType.from( implementation.getKind() ) ) );
            }
            piStatement.getClient().commitCurrentTransactionIfAuto();
            return resultBuilder.build();
        }
        throw new PIServiceException( "Can't execute a non DDL or non DML statement using this method..",
                "I9003",
                9002
        );
    }


    public StatementResult executeAndGetResult( PIStatement piStatement, int fetchSize ) {
        throwOnIllegalState( piStatement );
        Statement statement = piStatement.getStatement();
        PolyImplementation implementation = piStatement.getImplementation();
        PIClient client = piStatement.getClient();
        StatementResult.Builder resultBuilder = StatementResult.newBuilder();
        if ( Kind.DDL.contains( implementation.getKind() ) ) {
            resultBuilder.setScalar( 1 );
            return resultBuilder.build();
        }
        if ( Kind.DML.contains( implementation.getKind() ) ) {
            try ( ResultIterator iterator = implementation.execute( statement, -1 ) ) {
                resultBuilder.setScalar( PolyImplementation.getRowsChanged( statement, iterator.getIterator(), MonitoringType.from( implementation.getKind() ) ) );
            }
            client.commitCurrentTransactionIfAuto();
            return resultBuilder.build();
        }
        piStatement.setIterator( implementation.execute( implementation.getStatement(), fetchSize ) );
        Frame frame = fetch( piStatement, fetchSize );
        resultBuilder.setFrame( frame );
        if ( frame.getIsLast() ) {
            //TODO TH: special handling for result set updates. Do we need to wait with committing until all changes have been done?
            client.commitCurrentTransactionIfAuto();
        }
        return resultBuilder.build();
    }


    @Override
    public Frame fetch( PIStatement piStatement, int fetchSize ) {
        throwOnIllegalState( piStatement );
        StopWatch executionStopWatch = piStatement.getExecutionStopWatch();
        PolyImplementation implementation = piStatement.getImplementation();
        ResultIterator iterator = piStatement.getIterator();
        startOrResumeStopwatch( executionStopWatch );
        List<List<PolyValue>> rows = iterator.getNextBatch( fetchSize );
        executionStopWatch.suspend();
        boolean isLast = !iterator.hasMoreRows();
        if ( isLast ) {
            executionStopWatch.stop();
            implementation.getExecutionTimeMonitor().setExecutionTime( executionStopWatch.getNanoTime() );
        }
        List<ColumnMeta> columnMetas = RelationalMetaRetriever.retrieveColumnMetas( implementation );
        return PrismUtils.buildRelationalFrame( isLast, rows, columnMetas );
    }

}
