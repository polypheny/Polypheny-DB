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
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.monitoring.events.MonitoringType;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.PIServiceException;
import org.polypheny.db.prisminterface.relational.RelationalMetaRetriever;
import org.polypheny.db.prisminterface.statements.PIStatement;
import org.polypheny.db.prisminterface.utils.PrismUtils;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.prism.Frame;
import org.polypheny.prism.StatementResult;

final class GraphExecutor extends Executor {

    @Override
    DataModel getDataModel() {
        return DataModel.GRAPH;
    }


    @Override
    StatementResult executeAndGetResult( PIStatement statement ) {
        PolyImplementation implementation = statement.getImplementation();
        if ( implementation.isDDL() ) {
            return StatementResult.newBuilder().setScalar( 1 ).build();
        }
        if ( !Kind.DML.contains( implementation.getKind() ) || implementation.isDmlReturning() ) {
            throw new PIServiceException( "Cypher batch statements must update the graph without returning rows" );
        }
        try {
            long count;
            try ( ResultIterator iterator = implementation.execute( statement.getStatement(), -1 ) ) {
                count = PolyImplementation.getRowsChanged( statement.getStatement(), iterator.getIterator(), MonitoringType.from( implementation.getKind() ) );
            }
            statement.getClient().commitCurrentTransactionIfAuto();
            return StatementResult.newBuilder().setScalar( count ).build();
        } catch ( RuntimeException e ) {
            discardFailedResult( statement, e );
            throw e;
        }
    }


    @Override
    StatementResult executeAndGetResult( PIStatement statement, int fetchSize ) {
        PolyImplementation implementation = statement.getImplementation();
        if ( implementation.isDDL() || (Kind.DML.contains( implementation.getKind() ) && !implementation.isDmlReturning()) ) {
            return executeAndGetResult( statement );
        }
        try {
            statement.setGraphFrame( usesGraphFrame( implementation.tupleType, statement.getClient() ) );
            statement.setIterator( implementation.execute( statement.getStatement(), fetchSize ) );
            return StatementResult.newBuilder().setFrame( fetchFrame( statement, fetchSize ) ).build();
        } catch ( RuntimeException e ) {
            discardFailedResult( statement, e );
            throw e;
        }
    }


    @Override
    Frame fetch( PIStatement statement, int fetchSize ) {
        if ( statement.getIterator() == null || !statement.getTransaction().isActive() ) {
            throw new PIServiceException( "No open Cypher result exists for this statement" );
        }
        try {
            return fetchFrame( statement, fetchSize );
        } catch ( RuntimeException e ) {
            discardFailedResult( statement, e );
            throw e;
        }
    }


    private Frame fetchFrame( PIStatement statement, int fetchSize ) {
        StopWatch stopwatch = statement.getExecutionStopWatch();
        startOrResumeStopwatch( stopwatch );
        ResultIterator iterator = statement.getIterator();
        List<List<PolyValue>> rows = iterator.getNextBatch( fetchSize );
        boolean last = !iterator.hasMoreRows();
        boolean supportsGraphValues = statement.getClient().supportsGraphValues();
        Frame frame = statement.isGraphFrame()
                ? PrismUtils.buildGraphFrame( last, rows, supportsGraphValues )
                : PrismUtils.buildRelationalFrame( last, rows, RelationalMetaRetriever.retrieveColumnMetas( statement.getImplementation() ), supportsGraphValues );
        stopwatch.suspend();
        if ( last ) {
            stopwatch.stop();
            statement.getImplementation().getExecutionTimeMonitor().setExecutionTime( stopwatch.getNanoTime() );
            statement.closeResultsAndCommitIfAuto();
        }
        return frame;
    }


    private static boolean usesGraphFrame( AlgDataType rowType, PIClient client ) {
        if ( client.wantsRelationalCypherResults() ) {
            return false;
        }
        if ( isGraphType( rowType.getPolyType() ) ) {
            return true;
        }
        long graphColumns = rowType.getFields().stream().filter( field -> isGraphType( field.getType().getPolyType() ) ).count();
        if ( graphColumns == 0 ) {
            return false;
        }
        if ( graphColumns == rowType.getFieldCount() ) {
            return true;
        }
        if ( !client.supportsGraphValues() ) {
            throw new PIServiceException( "Cypher results mixing graph and scalar columns require Prism 2.3 or later." );
        }
        return false;
    }


    private static boolean isGraphType( PolyType type ) {
        return type == PolyType.GRAPH || type == PolyType.NODE || type == PolyType.EDGE || type == PolyType.PATH;
    }


    private static void discardFailedResult( PIStatement statement, RuntimeException failure ) {
        try {
            statement.closeResults();
        } catch ( RuntimeException cleanupFailure ) {
            failure.addSuppressed( cleanupFailure );
        }
        PIClient client = statement.getClient();
        if ( client.isAutoCommit() && client.getCurrentTransaction() == statement.getTransaction() ) {
            try {
                client.rollbackCurrentTransaction( "Cypher result could not be produced" );
            } catch ( RuntimeException cleanupFailure ) {
                failure.addSuppressed( cleanupFailure );
            }
        }
    }

}
