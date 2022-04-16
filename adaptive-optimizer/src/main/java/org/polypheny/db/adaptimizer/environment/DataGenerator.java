/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adaptimizer.environment;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyResult;
import org.polypheny.db.adaptimizer.except.TestDataGenerationException;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;

@Slf4j
public class DataGenerator {

    private final TransactionManager transactionManager;

    /**
     * Priority Queue where table-ids are ordered by their foreign key references to one another. If there
     * exists a foreign key in a table A referencing another table B then A < B. // Todo foreign key cycles?
     */
    private final PriorityQueue<Long> tableQueue;

    /**
     * Hash map mapping table-ids to their specified sizes after data generation.
     */
    private final HashMap<Long, Integer> tableSizes;

    /**
     * Map of column-ids to other column-ids. Contains a pair if column-id a references column-id b as a foreign key.
     */
    private final HashMap<Long, Long> columnReferences;

    private final Catalog catalog;
    private final int buffer;


    /**
     * Creates a Test Data Generator for a list of tables.
     * @param tables    tables to generate data for
     * @param sizes     sizes of tables after data generation.
     * @param buffer    how many records are inserted each iteration.
     */
    public DataGenerator( TransactionManager transactionManager, List<CatalogTable> tables, List<Integer> sizes, int buffer ) {
        catalog = Catalog.getInstance();

        this.transactionManager = transactionManager;

        // Order tables according to their foreign key references
        this.tableQueue = new PriorityQueue<>( ( tableIdA, tableIdB ) -> {
            Set<Long> referencesA = new HashSet<>();
            catalog.getForeignKeys( tableIdA ).forEach( foreignKey ->
                    referencesA.addAll( foreignKey.referencedKeyColumnIds )
            );

            Set<Long> referencesB = new HashSet<>();
            catalog.getForeignKeys( tableIdB ).forEach( foreignKey ->
                    referencesB.addAll( foreignKey.referencedKeyColumnIds )
            );

            long i = catalog.getColumns( tableIdA ).stream().map( column -> column.id ).filter( referencesB::contains ).count();
            long j = catalog.getColumns( tableIdB ).stream().map( column -> column.id ).filter( referencesA::contains ).count();

            // Todo there is no handling for cyclic foreign key references. Do they even exist?
            return Long.compare( i, j );

        } );

        List<Long> tableIds = tables.stream().map( table -> table.id ).collect( Collectors.toList());

        this.columnReferences = getColumnReferences( tableIds );

        this.tableSizes = ( HashMap<Long, Integer> ) IntStream.range( 0, tables.size() ).boxed()
                .collect( Collectors.toMap( tableIds::get, sizes::get ) );

        this.tableQueue.addAll( tableIds );

        this.buffer = buffer;

    }


    /**
     * Maps columns to other columns according to their foreign key references.
     * @param tableIds      List of table-ids.
     * @return              Hashmap where keys are column ids that are referencing the value column ids.
     */
    public HashMap<Long, Long> getColumnReferences( List<Long> tableIds ) {
        HashMap<Long, Long> referenceMap = new HashMap<>();
        for ( Long tableId : tableIds ) {
            List<CatalogForeignKey> foreignKeys = catalog.getForeignKeys( tableId );
            for ( CatalogForeignKey catalogForeignKey : foreignKeys ) {
                Long referenceTableId = catalogForeignKey.referencedKeyTableId;
                if ( ! tableIds.contains( referenceTableId ) ) {
                    break;
                }
                ImmutableList<Long> columnIds = catalogForeignKey.columnIds;
                ImmutableList<Long> referenceIds = catalogForeignKey.referencedKeyColumnIds;

                for ( int i = 0; i < columnIds.size(); i++ ) {
                    referenceMap.put( columnIds.get( i ), referenceIds.get( i ) );
                }
            }
        }
        return referenceMap;
    }


    /**
     * Generates Data and inserts it into the store.
     */
    public void generateData() {
        /*
        Generating random data for tables while keeping foreign key constraints, there
        needs to be a record for data stored in the referenced columns in order to
        choose viable options. Having the table-ids ordered according to their foreign
        key references we can simply add the necessary columns to a map and retrieve
        them if needed.
         */
        HashMap<Long, List<Object>> referenceDataMap = new HashMap<>();
        for ( Long id : this.columnReferences.keySet() ) {
            referenceDataMap.put( id, new ArrayList<>() );
        }

        // dequeue tables and fill
        while ( ! this.tableQueue.isEmpty() ) {

            Long tableId = this.tableQueue.remove();

            if ( log.isDebugEnabled() ) {
                log.debug( "Generating Data for table: {}", catalog.getTable( tableId ).name );
            }

            DataRecordSupplierBuilder testTableDataBuilder = new DataRecordSupplierBuilder( tableId );

            catalog.getTableKeys( tableId ).forEach( key -> {
                // For each key we add an option to the generated column: primary key implies unique values, foreign key implies predefined values.
                if ( catalog.isPrimaryKey( key.id ) ) {
                    key.columnIds.forEach( testTableDataBuilder::addPrimaryKeyOption );
                } else if ( catalog.isForeignKey( key.id ) ) {
                    List<Long> columnIds = ( ( CatalogForeignKey ) key ).referencedKeyColumnIds;
                    columnIds.forEach( columnId -> testTableDataBuilder.addForeignKeyOption( columnId, referenceDataMap.get( columnId ), false ) );
                }
            } );

            DataRecordSupplier testDataRecordSupplier = testTableDataBuilder.build();

            // Todo rewrite clumsy way of getting foreign key indexes and so on... ---
            List<Long> columnIds = testDataRecordSupplier.getColumnIds();
            List<CatalogColumn> catalogColumns = columnIds.stream().map( catalog::getColumn ).collect( Collectors.toList());
            List<Long> filteredIds = columnIds.stream().filter( this.columnReferences::containsKey ).collect( Collectors.toList());
            List<Integer> indexes = filteredIds.stream().map( columnIds::indexOf ).collect( Collectors.toList());
            CatalogTable catalogTable = catalog.getTable( tableId );
            // ---

            Transaction transaction;
            try {
                transaction = transactionManager.startTransaction(
                        catalog.getUser( Catalog.defaultUserId ).name,
                        catalog.getDatabase( Catalog.defaultDatabaseId ).name,
                        false,
                        null
                );
            } catch ( UnknownDatabaseException | GenericCatalogException | UnknownUserException | UnknownSchemaException e ) {
                e.printStackTrace();
                throw new TestDataGenerationException( "Could not start transaction", e );
            }

            Statement statement = transaction.createStatement();
            StringJoiner columns = new StringJoiner( ",", "\n\t(", ")\n" );
            catalogColumns.forEach( catalogColumn -> columns.add( "\"" + catalogColumn.name + "\"" ) );

            StringJoiner values;
            StringJoiner bulk;
            int rowCount = 0;
            int tableSize = this.tableSizes.get( tableId );
            while ( rowCount < tableSize ) {
                // Generate random records for buffer size
                List<List<Object>> data = Stream.generate( testDataRecordSupplier ).limit( buffer ).collect( Collectors.toList());


//                bulk = new StringJoiner( ",\n\t", "\n\t", "" );
                for ( List<Object> objects : data ) {
                    // Add foreign key values to referenceDataMap
                    for ( Integer index : indexes ) {
                        referenceDataMap.get( columnIds.get( index ) ).add( objects.get( index ) );
                    }

                    // Join values to Sql Query
//                    values = new StringJoiner( ",", "(", ")" );
//                    for ( int i = 0; i < objects.size(); i++ ) {
//                        Object value = objects.get( i );
//                        switch ( catalogColumns.get( i ).type ) {
//                            case TIMESTAMP:
//                                values.add( "\"" + value + "\"" );
//                                break;
//                            default:
//                                values.add( String.valueOf( value ) );
//                                break;
//                        }
//                    }
//                    bulk.add( values.toString() );
                }

                executeSqlInsert( transaction, catalogTable, catalogColumns, data );

//                String query = String.format( "INSERT INTO %s %s VALUES %s", DefaultTestEnvironment.SCHEMA_NAME + "." + catalogTable.name, columns, bulk );
//
//                if ( log.isDebugEnabled() ) {
//                    log.debug( "------------ Generated Query -----------\n" + query );
//                }
//
//                PolyResult polyResult;
//                try {
//                    Processor sqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.SQL );
//                    Node parsed = sqlProcessor.parse( query );
//                    QueryParameters parameters = new QueryParameters( query, SchemaType.RELATIONAL );
//                    Pair<Node, AlgDataType> validated = sqlProcessor.validate( statement.getTransaction(), parsed, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() );
//                    AlgRoot logicalRoot = sqlProcessor.translate( statement, validated.left, parameters );
//                    polyResult = statement.getQueryProcessor().prepareQuery( logicalRoot, true );
//
//                    polyResult.getRowsChanged( statement );
//
//                    transaction.commit();
//
//                } catch ( Exception | TransactionException e ) {
//                    log.info( "Generated query: {}", query );
//                    log.error( "Could not insert row", e );
//
//                    try {
//                        transaction.rollback();
//                    } catch ( TransactionException e2 ) {
//                        log.error( "Caught error while rolling back transaction", e2 );
//                    }
//
//                    throw new TestDataGenerationException( "Could not insert data into table", e );
//
//                }

                rowCount += buffer;

            }

        }

    }

    private void executeSqlInsert(Transaction transaction, CatalogTable catalogTable, List<CatalogColumn> catalogColumns, List<List<Object>> data) {


        StringJoiner columns = new StringJoiner( ",", "(", ")" );

        for ( CatalogColumn column : catalogColumns ) {
            columns.add( column.name );
        }

        StringJoiner values;
        for ( List<Object> objects : data ) {
            values = new StringJoiner( ",", "(", ")" );
            for ( int i = 0; i < objects.size(); i++ ) {
                Object value = objects.get( i );
                switch ( catalogColumns.get( i ).type ) {
                    case TIMESTAMP:
                        values.add( "\"" + value + "\"" );
                        break;
                    default:
                        values.add( String.valueOf( value ) );
                        break;
                }
            }

            String query = String.format( "INSERT INTO %s %s VALUES %s", DefaultTestEnvironment.SCHEMA_NAME + "." + catalogTable.name, columns, values );

            Statement statement = transaction.createStatement();
            Processor sqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.SQL );
            Node parsed = sqlProcessor.parse( query );
            QueryParameters parameters = new QueryParameters( query, SchemaType.RELATIONAL );
            Pair<Node, AlgDataType> validated = sqlProcessor.validate( statement.getTransaction(), parsed, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() );
            AlgRoot logicalRoot = sqlProcessor.translate( statement, validated.left, parameters );
            PolyResult polyResult = statement.getQueryProcessor().prepareQuery( logicalRoot, true );

            try {
                polyResult.getRowsChanged( statement );
            } catch ( Exception e ) {
                e.printStackTrace();
                log.error( "Could not execute statement.", e );
            }


        }

        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            e.printStackTrace();
            log.error( "Could not commit.", e );
            try {
                transaction.rollback();
            } catch ( TransactionException e2 ) {
                log.error( "Caught error while rolling back transaction", e2 );
            }

            throw new TestDataGenerationException( "Could not insert data into table", e );

        }

    }


}
