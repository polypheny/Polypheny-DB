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
import org.polypheny.db.algebra.constant.ResultKind;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.monitoring.events.MonitoringType;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.PIServiceException;
import org.polypheny.db.prisminterface.relational.RelationalMetaRetriever;
import org.polypheny.db.prisminterface.statements.PIStatement;
import org.polypheny.db.prisminterface.statements.PIStatement.ResultCursor;
import org.polypheny.db.prisminterface.utils.PrismUtils;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.Frame;
import org.polypheny.prism.Frame.ResultCase;
import org.polypheny.prism.StatementResult;


final class ResultExecutor {

    private ResultExecutor() {
    }


    static StatementResult executeCount( PIStatement statement ) {
        PolyImplementation implementation = implementation( statement );
        ResultKind result = implementation.getResultKind();
        DataModel model = statement.getLanguage().dataModel();
        if ( result == ResultKind.COMMAND ) {
            // Indexed SQL batch execution also consumes its command acknowledgment and completes autocommit.
            return model == DataModel.RELATIONAL ? updateCount( statement, implementation ) : scalar( 1 );
        }
        if ( result == ResultKind.UPDATE_COUNT ) {
            return updateCount( statement, implementation );
        }
        if ( model == DataModel.GRAPH ) {
            throw new PIServiceException( "Cypher batch statements must update the graph without returning rows" );
        }
        throw new PIServiceException( "Can't execute a non DDL or non DML statement using this method.", "I9003", 9002 );
    }


    static StatementResult execute( PIStatement statement, int fetchSize ) {
        PolyImplementation implementation = implementation( statement );
        ResultKind result = implementation.getResultKind();
        if ( result == ResultKind.COMMAND ) {
            return scalar( 1 );
        }
        if ( result == ResultKind.UPDATE_COUNT && statement.getLanguage().dataModel() != DataModel.DOCUMENT ) {
            return updateCount( statement, implementation );
        }

        Transaction transaction = statement.getTransaction();
        try {
            ResultCase format = selectFrame( statement, implementation );
            List<ColumnMeta> columns = format == ResultCase.RELATIONAL_FRAME
                    ? RelationalMetaRetriever.retrieveColumnMetas( implementation )
                    : List.of();
            ResultIterator iterator = implementation.execute( statement.getStatement(), fetchSize );
            ResultCursor cursor = new ResultCursor( iterator, transaction, format, columns );
            statement.setResultCursor( cursor );
            return StatementResult.newBuilder().setFrame( readFrame( statement, cursor, fetchSize ) ).build();
        } catch ( RuntimeException e ) {
            discardFailedResult( statement, transaction, e );
            throw e;
        }
    }


    static Frame fetch( PIStatement statement, int fetchSize ) {
        ResultCursor cursor = statement.getResultCursor();
        if ( cursor == null || !cursor.transaction().isActive() ) {
            if ( isGraphStatement( statement ) ) {
                throw new PIServiceException( "No open Cypher result exists for this statement" );
            }
            throw new PIServiceException( "No open result exists for this statement", "I9002", 9002 );
        }
        try {
            return readFrame( statement, cursor, fetchSize );
        } catch ( RuntimeException e ) {
            discardFailedResult( statement, cursor.transaction(), e );
            throw e;
        }
    }


    private static ResultCase selectFrame( PIStatement statement, PolyImplementation implementation ) {
        return switch ( implementation.getResultKind() ) {
            case DOCUMENTS, UPDATE_COUNT -> ResultCase.DOCUMENT_FRAME;
            case GRAPH_ELEMENTS -> isGraphStatement( statement ) && statement.getClient().wantsRelationalCypherResults()
                    ? ResultCase.RELATIONAL_FRAME
                    : ResultCase.GRAPH_FRAME;
            case ROWS -> {
                // Prism 2.2 can decode a graph stream. Mixed Cypher projections use graph values in row cells.
                if ( isGraphStatement( statement ) && !statement.getClient().supportsGraphValues()
                        && implementation.tupleType.getFields().stream().anyMatch( field -> ResultKind.isGraphType( field.getType().getPolyType() ) ) ) {
                    throw new PIServiceException( "Cypher results mixing graph and scalar columns require Prism 2.3 or later." );
                }
                yield ResultCase.RELATIONAL_FRAME;
            }
            case COMMAND -> throw new IllegalStateException( "Command acknowledgments do not have frames" );
        };
    }


    private static Frame readFrame( PIStatement statement, ResultCursor cursor, int fetchSize ) {
        StopWatch stopwatch = statement.getExecutionStopWatch();
        if ( stopwatch.isSuspended() ) {
            stopwatch.resume();
        } else if ( stopwatch.isStopped() ) {
            stopwatch.reset();
            stopwatch.start();
        }

        ResultIterator iterator = cursor.iterator();
        List<List<PolyValue>> rows = iterator.getNextBatch( fetchSize );
        boolean last = !iterator.hasMoreRows();
        boolean supportsGraphValues = statement.getClient().supportsGraphValues();
        Frame frame = switch ( cursor.format() ) {
            case RELATIONAL_FRAME -> PrismUtils.buildRelationalFrame( last, rows, cursor.columns(), supportsGraphValues );
            case GRAPH_FRAME -> PrismUtils.buildGraphFrame( last, rows, supportsGraphValues );
            case DOCUMENT_FRAME -> {
                List<PolyValue> documents = rows.stream().map( row -> row.get( 0 ) ).toList();
                // Paged MQL writes have historically exposed their count as a document.
                if ( iterator.getImplementation().getResultKind() == ResultKind.UPDATE_COUNT
                        && documents.size() == 1 && documents.get( 0 ).isLong() ) {
                    documents = List.of( new PolyDocument( PolyString.of( "updateCount" ), documents.get( 0 ) ) );
                }
                yield PrismUtils.buildDocumentFrame( last, documents, supportsGraphValues );
            }
            default -> throw new IllegalStateException( "Unexpected Prism result format " + cursor.format() );
        };

        if ( last ) {
            stopwatch.stop();
            iterator.getExecutionTimeMonitor().setExecutionTime( stopwatch.getNanoTime() );
            statement.closeResultsAndCommitIfAuto();
        } else {
            stopwatch.suspend();
        }
        return frame;
    }


    private static StatementResult updateCount( PIStatement statement, PolyImplementation implementation ) {
        Transaction transaction = statement.getTransaction();
        try {
            long count;
            try ( ResultIterator iterator = implementation.execute( statement.getStatement(), -1 ) ) {
                count = PolyImplementation.getRowsChanged( statement.getStatement(), iterator.getIterator(), MonitoringType.from( implementation.getKind() ) );
            }
            statement.getClient().getStatementManager().commitIfAutocommitAndNoOpenResults( transaction );
            return scalar( count );
        } catch ( RuntimeException e ) {
            discardFailedResult( statement, transaction, e );
            throw e;
        }
    }


    private static StatementResult scalar( long count ) {
        return StatementResult.newBuilder().setScalar( count ).build();
    }


    private static PolyImplementation implementation( PIStatement statement ) {
        PolyImplementation implementation = statement.getImplementation();
        if ( statement.getStatement() == null ) {
            throw new PIServiceException( "Statement is not linked to a polypheny statement", "I9001", 9001 );
        }
        if ( implementation == null ) {
            throw new PIServiceException( "Can't retrieve results from an unprepared statement.", "I9002", 9002 );
        }
        return implementation;
    }


    private static boolean isGraphStatement( PIStatement statement ) {
        return statement.getLanguage().dataModel() == DataModel.GRAPH;
    }


    private static void discardFailedResult( PIStatement statement, Transaction transaction, RuntimeException failure ) {
        try {
            statement.closeResults();
        } catch ( RuntimeException cleanupFailure ) {
            failure.addSuppressed( cleanupFailure );
        }
        PIClient client = statement.getClient();
        try {
            if ( client.isAutoCommit() ) {
                client.rollbackAutocommitTransaction( transaction, failure.getMessage() );
            } else if ( isGraphStatement( statement ) && Kind.DML.contains( statement.getImplementation().getKind() )
                    && client.getCurrentTransaction() == transaction ) {
                // A Cypher write may reach the store before a later clause fails. Prism has no statement savepoint.
                client.rollbackCurrentTransaction( failure.getMessage() );
            }
        } catch ( RuntimeException cleanupFailure ) {
            failure.addSuppressed( cleanupFailure );
        }
    }

}
