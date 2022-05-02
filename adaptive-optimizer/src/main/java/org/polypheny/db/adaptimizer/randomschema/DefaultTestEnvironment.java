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

package org.polypheny.db.adaptimizer.randomschema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.Catalog.ForeignKeyOption;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.TableAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ColumnInformation;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.ddl.exception.PartitionGroupNamesNotUniqueException;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;


/**
 * Utility Class to create the default test environment for the adaptimizer module.
 */
@Slf4j
public class DefaultTestEnvironment {

    @Setter
    private static TransactionManager transactionManager;

    private static AdapterManager adapterManager;
    private static DdlManager ddlManager;

    public static final String SCHEMA_NAME = "adapt";
    public static final String HSQLDB_ADAPTER_NAME = "adapt_hsqldb_adapter";

    // Table IDs
    public static CatalogTable customers;
    public static CatalogTable orders;
    public static CatalogTable products;
    public static CatalogTable shipments;
    public static CatalogTable purchases;


    private static DataStore addHsqldbAdapter() {
        Map<String, String> hsqldbSettings = new HashMap<>();
        hsqldbSettings.put( "type", "Memory" );
        hsqldbSettings.put( "tableType", "Memory" );
        hsqldbSettings.put( "path", "maxConnections" );
        hsqldbSettings.put( "maxConnections", "25" );
        hsqldbSettings.put( "trxControlMode", "mvcc" );
        hsqldbSettings.put( "trxIsolationLevel", "read_committed" );
        hsqldbSettings.put( "mode", "embedded" );
        return ( DataStore ) adapterManager.addAdapter(
                "org.polypheny.db.adapter.jdbc.stores.HsqldbStore", HSQLDB_ADAPTER_NAME, hsqldbSettings
        );
    }


    public static class TableColumnData {

        private final Pair<List<ColumnInformation>, List<ConstraintInformation>> columns;
        private final long schemaId;
        private final String tableName;
        private final List<DataStore> stores;
        private final PlacementType placementType;
        private final Statement statement;

        private int position;


        public TableColumnData( long schemaId, String tableName, List<DataStore> stores, PlacementType placementType, Statement statement ) {
            this.columns = new Pair<>( new ArrayList<>(), new ArrayList<>() );
            this.schemaId = schemaId;
            this.tableName = tableName;
            this.stores = stores;
            this.placementType = placementType;
            this.statement = statement;

            this.position = 0;
        }


        public TableColumnData addColumn(
                String columnName, PolyType type, PolyType collectionType,
                Integer precision, Integer scale, Integer dimension, Integer cardinality, boolean nullable, Collation collation, String defaultValue ) {
            this.columns.left.add(
                    new ColumnInformation(
                            columnName,
                            new ColumnTypeInformation(
                                    type,
                                    collectionType,
                                    precision,
                                    scale,
                                    dimension,
                                    cardinality,
                                    nullable
                            ),
                            collation,
                            defaultValue,
                            position++
                    )
            );
            return this;
        }


        public TableColumnData addConstraint(
                String constraintName, ConstraintType constraintType,
                List<String> columnNames ) {
            this.columns.right.add(
                    new ConstraintInformation(
                            constraintName,
                            constraintType,
                            columnNames
                    )
            );
            return this;
        }


        public void create() {
            try {
                ddlManager.createTable( this.schemaId, this.tableName, this.columns.left, this.columns.right, true, this.stores, this.placementType, this.statement );
            } catch ( TableAlreadyExistsException | ColumnNotExistsException | UnknownPartitionTypeException | UnknownColumnException | PartitionGroupNamesNotUniqueException e ) {
                log.debug( "Could not create table... {}", this.tableName );
                e.printStackTrace();
            }
        }


    }


    public static String generate() {
        Catalog catalog = Catalog.getInstance();
        adapterManager = AdapterManager.getInstance();
        ddlManager = DdlManager.getInstance();

        List<DataStore> stores = List.of( addHsqldbAdapter() );

        Transaction transaction;
        try {
            transaction = transactionManager.startTransaction( catalog.getUser( Catalog.defaultUserId ).name, catalog.getDatabase( Catalog.defaultDatabaseId ).name, false, "DefaultTestEnvironment-Adaptimizer" );
        } catch ( Exception e ) {
            e.printStackTrace();
            return "Could not create transaction";
        }

        try {

            ddlManager.createSchema( SCHEMA_NAME, Catalog.defaultDatabaseId, SchemaType.RELATIONAL, Catalog.defaultUserId, true, false );
            CatalogSchema catalogSchema = catalog.getSchema( Catalog.defaultDatabaseId, SCHEMA_NAME );

            new TableColumnData( catalogSchema.id, "customers", stores, PlacementType.MANUAL, transaction.createStatement() )
                    .addColumn( "customer_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "customer_name", PolyType.VARCHAR, null, 24, null, null, null, false, Collation.CASE_INSENSITIVE, null )
                    .addColumn( "customer_address", PolyType.VARCHAR, null, 24, null, null, null, false, Collation.CASE_INSENSITIVE, null )
                    .addColumn( "customer_phone", PolyType.VARCHAR, null, 24, null, null, null, false, Collation.CASE_INSENSITIVE, null )
                    .addConstraint( "customers_pk", ConstraintType.PRIMARY, List.of( "customer_id" ) )
                    .create();

            new TableColumnData( catalogSchema.id, "orders", stores, PlacementType.MANUAL, transaction.createStatement() )
                    .addColumn( "order_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "customer_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "order_date", PolyType.TIMESTAMP, null, null, null, null, null, false, null, null )
                    .addColumn( "has_shipped", PolyType.BOOLEAN, null, null, null, null, null, false, null, null )
                    .addConstraint( "orders_pk", ConstraintType.PRIMARY, List.of( "order_id" ) )
                    .create();

            new TableColumnData( catalogSchema.id, "products", stores, PlacementType.MANUAL, transaction.createStatement() )
                    .addColumn( "product_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "product_name", PolyType.VARCHAR, null, 24, null, null, null, false, Collation.CASE_INSENSITIVE, null )
                    .addColumn( "product_stock", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "product_price", PolyType.DOUBLE, null, null, null, null, null, false, null, null )
                    .addConstraint( "products_pk", ConstraintType.PRIMARY, List.of( "product_id" ) )
                    .create();

            new TableColumnData( catalogSchema.id, "shipments", stores, PlacementType.MANUAL, transaction.createStatement() )
                    .addColumn( "shipment_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "order_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "shipment_date", PolyType.DATE, null, null, null, null, null, false, null, null )
                    .addConstraint( "shipments_pk", ConstraintType.PRIMARY, List.of( "shipment_id" ) )
                    .create();

            new TableColumnData( catalogSchema.id, "purchases", stores, PlacementType.MANUAL, transaction.createStatement() )
                    .addColumn( "purchase_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "order_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "product_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "quantity", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addConstraint( "purchases_pk", ConstraintType.PRIMARY, List.of( "purchase_id" ) )
                    .create();

            customers = catalog.getTable( catalogSchema.id, "customers" );
            orders = catalog.getTable( catalogSchema.id, "orders" );
            products = catalog.getTable( catalogSchema.id, "products" );
            shipments = catalog.getTable( catalogSchema.id, "shipments" );
            purchases = catalog.getTable( catalogSchema.id, "purchases" );

            ddlManager.addForeignKey( orders, customers, List.of( "customer_id" ), List.of( "customer_id" ), "customer_orders_fk", ForeignKeyOption.NONE, ForeignKeyOption.NONE );
            ddlManager.addForeignKey( shipments, orders, List.of( "order_id" ), List.of( "order_id" ), "order_shipped_fk", ForeignKeyOption.NONE, ForeignKeyOption.NONE );
            ddlManager.addForeignKey( purchases, orders, List.of( "order_id" ), List.of( "order_id" ), "order_purchased_fk", ForeignKeyOption.NONE, ForeignKeyOption.NONE );
            ddlManager.addForeignKey( purchases, products, List.of( "product_id" ), List.of( "product_id" ), "product_purchased_fk", ForeignKeyOption.NONE, ForeignKeyOption.NONE );

        } catch ( Exception e ) {
            log.debug( "Default environment could not be created", e );
            try {
                transaction.rollback();
            } catch ( TransactionException ex ) {
                log.debug( "Could not rollback transaction", ex );
            }
            return "Failed to create tables";
        }

        try {
            transaction.commit();
            return "Done.";
        } catch ( TransactionException e ) {
            log.debug( "Could not commit transaction", e );
            return "Transaction not committed.";
        }

    }


}
