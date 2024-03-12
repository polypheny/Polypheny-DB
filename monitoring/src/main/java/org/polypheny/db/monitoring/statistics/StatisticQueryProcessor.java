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

package org.polypheny.db.monitoring.statistics;


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.entity.PolyValue;


@Slf4j
public class StatisticQueryProcessor {

    @Getter
    private final TransactionManager transactionManager;


    /**
     * LowCostQueries can be used to retrieve short answered queries
     * Idea is to expose a selected list of sql operations with a small list of results and not impact performance
     */
    public StatisticQueryProcessor( final TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
    }


    public StatisticQueryProcessor( TransactionManager transactionManager, Authenticator authenticator ) {
        this( transactionManager );
    }


    /**
     * Handles the request for one columns stats
     *
     * @return result of the query
     */
    public StatisticQueryResult selectOneColumnStat( AlgNode node, Transaction transaction, Statement statement, QueryResult queryResult ) {
        StatisticResult res = this.executeColStat( node, transaction, statement, queryResult );
        if ( res.getColumns() != null && res.getColumns().length == 1 ) {
            return res.getColumns()[0];
        }
        return null;
    }


    /**
     * Gets all columns in the database
     *
     * @return all the columns
     */
    public List<QueryResult> getAllColumns() {
        Snapshot snapshot = Catalog.getInstance().getSnapshot();
        return snapshot.getNamespaces( null )
                .stream()
                .filter( n -> n.dataModel == DataModel.RELATIONAL )
                .flatMap( n -> snapshot.rel().getTables( Pattern.of( n.name ), null ).stream().filter( t -> t.entityType != EntityType.VIEW ).flatMap( t -> snapshot.rel().getColumns( t.id ).stream() ) )
                .map( QueryResult::fromCatalogColumn )
                .toList();
    }


    /**
     * Gets all tables in the database
     *
     * @return all the tables ids
     */
    public List<LogicalTable> getAllRelEntites() {
        Snapshot snapshot = Catalog.getInstance().getSnapshot();
        return snapshot.getNamespaces( null ).stream().filter( n -> n.dataModel == DataModel.RELATIONAL )
                .flatMap( n -> snapshot.rel().getTables( Pattern.of( n.name ), null ).stream().filter( t -> t.entityType != EntityType.VIEW ) ).collect( Collectors.toList() );
    }


    /**
     * Get all columns of a specific table
     *
     * @return all columns
     */
    public List<QueryResult> getAllColumns( Long tableId ) {
        Snapshot snapshot = Catalog.getInstance().getSnapshot();
        return snapshot.getNamespaces( null ).stream().flatMap( n -> snapshot.rel().getColumns( tableId ).stream() ).map( QueryResult::fromCatalogColumn ).collect( Collectors.toList() );
    }


    public void commitTransaction( Transaction transaction, Statement statement ) {
        try {
            // Locks are released within commit
            transaction.commit();
        } catch ( TransactionException e ) {
            log.error( "Caught exception while executing a query from the console", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.error( "Caught exception while rollback", e );
            }
        } finally {
            // Release lock
            statement.getQueryProcessor().unlock( statement );
        }
    }


    private StatisticResult executeColStat( AlgNode node, Transaction transaction, Statement statement, QueryResult queryResult ) {
        StatisticResult result = new StatisticResult();

        try {
            result = executeColStat( statement, node, queryResult );
        } catch ( QueryExecutionException e ) {
            log.error( "Caught exception while executing a query from the console", e );
        }
        return result;
    }

    // -----------------------------------------------------------------------
    //                                Helper
    // -----------------------------------------------------------------------


    private StatisticResult executeColStat( Statement statement, AlgNode node, QueryResult queryResult ) throws QueryExecutionException {
        PolyImplementation implementation;
        List<List<PolyValue>> rows;

        try {
            implementation = statement.getQueryProcessor().prepareQuery( AlgRoot.of( node, Kind.SELECT ), node.getTupleType(), false );
            ResultIterator iterator = implementation.execute( statement, getPageSize() );
            rows = iterator.getNextBatch();
            iterator.close();
        } catch ( Throwable t ) {
            throw new QueryExecutionException( t );
        }

        List<PolyValue[]> data = new ArrayList<>();
        for ( List<PolyValue> row : rows ) {
            PolyValue[] temp = new PolyValue[row.size()];
            int counter = 0;
            for ( PolyValue o : row ) {
                temp[counter] = o;
                counter++;
            }
            data.add( temp );
        }

        PolyValue[][] d = data.toArray( new PolyValue[0][] );

        return new StatisticResult( queryResult, d );
    }


    /**
     * Get the page
     */
    private int getPageSize() {
        return RuntimeConfig.UI_PAGE_SIZE.getInteger();
    }


    static class QueryExecutionException extends Exception {

        QueryExecutionException( Throwable t ) {
            super( t );
        }

    }

}
