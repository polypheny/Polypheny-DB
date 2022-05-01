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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
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
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
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

    private final Catalog catalog;
    private final TransactionManager transactionManager;
    private final List<DataTableOptionTemplate> tables;
    private final Queue<DataTableOptionTemplate> tableQueue;
    private final int buffer;

    /**
     * Creates a Test Data Generator for a list of tables.
     * @param tables    tables to generate data for in the form of a list of {@link DataTableOptionTemplate}
     * @param buffer    how many records are inserted each iteration.
     */
    public DataGenerator( TransactionManager transactionManager, List<DataTableOptionTemplate> tables, int buffer ) {
        this.catalog = Catalog.getInstance();
        this.transactionManager = transactionManager;
        this.tables = tables;

        this.tableQueue = new ArrayDeque<>();
        this.tableQueue.addAll( tables );

        this.buffer = buffer;
    }


    private static DataTableOptionTemplate getTemplateForTableId( List<DataTableOptionTemplate> tables, long tableId ) {
        for ( DataTableOptionTemplate dataTableOptionTemplate : tables ) {
            if ( dataTableOptionTemplate.hasTableId( tableId ) ) {
                return dataTableOptionTemplate;
            }
        }
        return null;
    }


    /**
     * Maps columns to other columns according to their foreign key references.
     * @param tables     List of {@link DataTableOptionTemplate}
     */
    private void searchColumnReferences( List<DataTableOptionTemplate> tables ) {

        for ( DataTableOptionTemplate template : tables ) {
            List<CatalogForeignKey> foreignKeys = catalog.getForeignKeys( template.getCatalogTable().id );

            for ( CatalogForeignKey catalogForeignKey : foreignKeys ) {
                DataTableOptionTemplate referencedTemplate = getTemplateForTableId( tables, catalogForeignKey.referencedKeyTableId );

                if ( referencedTemplate == null ) {
                    // Only consider references for tables passed to the Generator.
                    break;
                }

                template.addReferencingColumns( catalogForeignKey.columnIds, catalogForeignKey.referencedKeyColumnIds );
                referencedTemplate.addReferencedColumnIndexes( catalogForeignKey.referencedKeyColumnIds );
            }
        }

    }


    /**
     * Generates Data and inserts it into the store.
     */
    public void generateData() {

        // Search all references
        this.searchColumnReferences( this.tables );

        /*
        Generating random data for tables while keeping foreign key constraints, there
        needs to be a record for data stored in the referenced columns in order to
        choose viable options. Having the table-ids ordered according to their foreign
        key references we can simply add the necessary columns to a map and retrieve
        them if needed.
         */
        HashMap<Long, List<Object>> referenceDataMap = new HashMap<>();

        // Counter for dequeue operations
        int iteration = 0;

        // dequeue tables and fill
        while ( ! this.tableQueue.isEmpty() ) {

            DataTableOptionTemplate template = this.tableQueue.remove();
            iteration++;

            if ( log.isDebugEnabled() ) {
                log.debug( "Considering Table {}", catalog.getTable( template.getTableId() ).name );
            }

            //  First we build a DataRecordSupplier that will give random rows / records for the table in consideration.
            DataRecordSupplierBuilder testRecordSupplierBuilder = new DataRecordSupplierBuilder( catalog, template );


            // Add all primary key options, these columns will have unique values.
            for ( CatalogKey key : catalog.getTableKeys( template.getTableId() )) {
                if ( catalog.isPrimaryKey( key.id ) ) {
                    key.columnIds.forEach( testRecordSupplierBuilder::addPrimaryKeyOption );
                }
            }

            // In the case a table references another we have to postpone that table until the referenced table is generated.
            boolean postpone = false;
            for ( Pair<Long, Long> reference : template.getReferences() ) {
                if ( ! referenceDataMap.containsKey( reference.right ) ) {
                    postpone = true;
                    break;
                } else {
                    testRecordSupplierBuilder.addForeignKeyOption( reference.left, referenceDataMap.get( reference.right ), false );
                }
            }

            // If we have too many iterations there are cyclic foreign key references.
            if ( iteration > this.tables.size() * this.tables.size() ) {
                throw new TestDataGenerationException( "Too many iterations for data generation, are there cyclic foreign key references in the tables?",
                        new IllegalArgumentException( "Invalid input tables." ) );
            }

            if ( postpone ) {
                this.tableQueue.add( template );
                continue; // end iteration here
            }

            generateDataForTable( template, testRecordSupplierBuilder.build(), referenceDataMap );

        }

    }


    private void generateDataForTable( DataTableOptionTemplate template, DataRecordSupplier dataRecordSupplier, HashMap<Long, List<Object>> referenceDataMap ) {

        Transaction transaction = this.getTransaction();
        template.getReferencedColumnIds().forEach( ( pair ) -> referenceDataMap.put( pair.left, new ArrayList<>() ) );

        int rowCount = 0;
        while ( rowCount <= template.getSize() ) {
            // Generate random records for buffer size
            List<List<Object>> data = Stream.generate( dataRecordSupplier ).limit(
                    ( rowCount + buffer > template.getSize() ) ? rowCount + buffer - template.getSize() : buffer
            ).collect( Collectors.toList());

            for ( List<Object> objects : data ) {
                // Add foreign key values to referenceDataMap
                for ( Pair<Long, Integer> referencedColumn : template.getReferencedColumnIds() ) {
                    referenceDataMap.get( referencedColumn.left ).add( objects.get( referencedColumn.right ) );
                }
            }

            String query = getInsertQuery( template.getCatalogTable(), template.getCatalogColumns(), data );
            this.executeSqlInsert( transaction.createStatement(), query );

            rowCount += buffer;
        }

        this.commitTransaction( transaction );

    }


    private String getInsertQuery( CatalogTable catalogTable, List<CatalogColumn> catalogColumns, List<List<Object>> data ) {
        StringJoiner columns = new StringJoiner( ",", "(", ")" );

        for ( CatalogColumn column : catalogColumns ) {
            columns.add( column.name );
        }

        StringJoiner bulk = new StringJoiner( ",", "", "" );
        StringJoiner values;
        for ( List<Object> objects : data ) {
            values = new StringJoiner( ",", "(", ")" );
            for ( int i = 0; i < objects.size(); i++ ) {
                Object value = objects.get( i );
                switch ( catalogColumns.get( i ).type ) {
                    case TIMESTAMP:
                        values.add( "timestamp '" + value + "'" );
                        break;
                    case DATE:
                        values.add( "date '" + value + "'" );
                        break;
                    case TIME:
                        values.add( "time '" + value + "'" );
                        break;
                    case CHAR:
                    case VARCHAR:
                        values.add( "'" + value + "'" );
                        break;
                    default:
                        values.add( String.valueOf( value ) );
                        break;
                }
            }
            bulk.add( values.toString() );
        }

        return String.format( "insert into %s %s values %s", DefaultTestEnvironment.SCHEMA_NAME + "." + catalogTable.name, columns, bulk );
    }


    private Transaction getTransaction() {
        Transaction transaction;
        try {
            transaction = transactionManager.startTransaction(
                    catalog.getUser( Catalog.defaultUserId ).name,
                    catalog.getDatabase( Catalog.defaultDatabaseId ).name,
                    false,
                    "Adaptimizer - DataGenerator"
            );
        } catch ( UnknownDatabaseException | GenericCatalogException | UnknownUserException | UnknownSchemaException e ) {
            e.printStackTrace();
            throw new TestDataGenerationException( "Could not start transaction", e );
        }
        return transaction;
    }


    private void commitTransaction(Transaction transaction) {
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


    private void executeSqlInsert(Statement statement, String insertQuery ) {
        Processor sqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.SQL );
        Pair<Node, AlgDataType> validated = sqlProcessor.validate( statement.getTransaction(), sqlProcessor.parse( insertQuery ), false );
        AlgRoot logicalRoot = sqlProcessor.translate( statement, validated.left, new QueryParameters( insertQuery, SchemaType.RELATIONAL ) );
        PolyResult polyResult = statement.getQueryProcessor().prepareQuery( logicalRoot, false );

        try {
            polyResult.getRowsChanged( statement );
        } catch ( Exception e ) {
            throw new TestDataGenerationException( "Could not execute insert query", e );
        }

    }

}
