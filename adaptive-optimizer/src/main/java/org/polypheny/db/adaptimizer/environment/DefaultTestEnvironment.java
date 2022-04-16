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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.ForeignKeyOption;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.PlacementAlreadyExistsException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;


/**
 * Utility Class to create the default test environment for the adaptimizer module.
 */
@Slf4j
public class DefaultTestEnvironment {
    @Setter
    private static TransactionManager transactionManager;

    private static Catalog catalog;
    private static AdapterManager adapterManager;
    private static DdlManager ddlManager;

    public static final String SCHEMA_NAME = "adapt";
    public static final String HSQLDB_ADAPTER_NAME = "adapt_hsqldb_adapter";

    public static final String CUSTOMERS_TABLE_NAME = "customers";
    public static final String ORDERS_TABLE_NAME = "orders";
    public static final String PRODUCTS_TABLE_NAME = "products";
    public static final String SHIPMENTS_TABLE_NAME = "shipments";
    public static final String PURCHASES_TABLE_NAME = "purchases";

    // Schema ID
    public static Long SCHEMA_ID;

    // Adapter IDs
    public static Integer HSQLDB_ADAPTER_ID;

    // Table IDs
    public static Long CUSTOMERS_TABLE_ID;
    public static Long ORDERS_TABLE_ID;
    public static Long PRODUCTS_TABLE_ID;
    public static Long SHIPMENTS_TABLE_ID;
    public static Long PURCHASES_TABLE_ID;

    // Customers Column IDs
    public static Long CUSTOMERS_COL_ID_CUSTOMER_ID;
    public static Long CUSTOMERS_COL_ID_CUSTOMER_NAME;
    public static Long CUSTOMERS_COL_ID_CUSTOMER_PHONE;
    public static Long CUSTOMERS_COL_ID_CUSTOMER_ADDRESS;

    // Orders Column IDs
    public static Long ORDERS_COL_ID_ORDER_ID;
    public static Long ORDERS_COL_ID_CUSTOMER_ID;
    public static Long ORDERS_COL_ID_ORDER_DATE;
    public static Long ORDERS_COL_ID_HAS_SHIPPED;

    // Products Column IDs
    public static Long PRODUCTS_COL_ID_PRODUCT_ID;
    public static Long PRODUCTS_COL_ID_PRODUCT_NAME;
    public static Long PRODUCTS_COL_ID_PRODUCT_STOCK;
    public static Long PRODUCTS_COL_ID_PRODUCT_PRICE;

    // Shipments Column IDs
    public static Long SHIPMENTS_COL_ID_SHIPMENT_ID;
    public static Long SHIPMENTS_COL_ID_ORDER_ID;
    public static Long SHIPMENTS_COL_ID_SHIPMENT_DATE;

    // Purchases Column IDs
    public static Long PURCHASES_COL_ID_PURCHASE_ID;
    public static Long PURCHASES_COL_ID_ORDER_ID;
    public static Long PURCHASES_COL_ID_PRODUCT_ID;
    public static Long PURCHASES_COL_ID_QUANTITY;


    private static Adapter addHsqldbAdapter() {
        Map<String, String> hsqldbSettings = new HashMap<>();
        hsqldbSettings.put( "type", "Memory" );
        hsqldbSettings.put( "tableType", "Memory" );
        hsqldbSettings.put( "path", "maxConnections" );
        hsqldbSettings.put( "maxConnections", "25" );
        hsqldbSettings.put( "trxControlMode", "mvcc" );
        hsqldbSettings.put( "trxIsolationLevel", "read_committed" );
        hsqldbSettings.put( "mode", "embedded" );
        return adapterManager.addAdapter(
                "org.polypheny.db.adapter.jdbc.stores.HsqldbStore", HSQLDB_ADAPTER_NAME, hsqldbSettings
        );
    }

    private static void addDataPlacements( Adapter adapter, List<Long> tableIds, Transaction transaction ) {
        tableIds.forEach( tableId -> {
            CatalogTable catalogTable = catalog.getTable( tableId );
            try {
                ddlManager.addDataPlacement(
                        catalogTable,
                        catalog.getColumns( tableId ).stream().map( column -> column.id ).collect( Collectors.toList()),
                        new LinkedList<>(),
                        new LinkedList<>(),
                        (DataStore) adapter,
                        transaction.createStatement(),
                        false
                );
            } catch ( PlacementAlreadyExistsException e ) {
                e.printStackTrace();
            }
        } );
    }


    public static String generate() {
        catalog = Catalog.getInstance();
        adapterManager = AdapterManager.getInstance();
        ddlManager = DdlManager.getInstance();

        if ( catalog.checkIfExistsSchema( Catalog.defaultDatabaseId, SCHEMA_NAME ) ) {
            return "Already executed.";
        }

        // Generate Default Test Environment Schema
        // Add Schema
        SCHEMA_ID = catalog.addSchema( SCHEMA_NAME, Catalog.defaultDatabaseId, Catalog.defaultUserId, SchemaType.RELATIONAL );

        // Customers Table
        CUSTOMERS_TABLE_ID = catalog.addTable( CUSTOMERS_TABLE_NAME, SCHEMA_ID, Catalog.defaultUserId, TableType.TABLE, true );

        int columnPos = 0;
        // Add columns on Customers Table
        CUSTOMERS_COL_ID_CUSTOMER_ID = catalog.addColumn(
                "customer_id", CUSTOMERS_TABLE_ID, columnPos++, PolyType.INTEGER, null, null, null,
                null, null, false, null
        );
        CUSTOMERS_COL_ID_CUSTOMER_NAME = catalog.addColumn(
                "customer_name", CUSTOMERS_TABLE_ID, columnPos++, PolyType.VARCHAR, null, 24, null,
                null, null, false, Collation.CASE_INSENSITIVE
        );
        CUSTOMERS_COL_ID_CUSTOMER_PHONE = catalog.addColumn(
                "customer_phone", CUSTOMERS_TABLE_ID, columnPos++, PolyType.VARCHAR, null, 24, null,
                null, null, false, Collation.CASE_INSENSITIVE
        );
        CUSTOMERS_COL_ID_CUSTOMER_ADDRESS = catalog.addColumn(
                "customer_address", CUSTOMERS_TABLE_ID, columnPos, PolyType.VARCHAR, null, 24, null,
                null, null, false, Collation.CASE_INSENSITIVE
        );

        try {
            catalog.addPrimaryKey( CUSTOMERS_TABLE_ID, List.of( CUSTOMERS_COL_ID_CUSTOMER_ID ) );
        } catch ( GenericCatalogException e ) {
            e.printStackTrace();
        }

        // Orders Table
        ORDERS_TABLE_ID = catalog.addTable( ORDERS_TABLE_NAME, SCHEMA_ID, Catalog.defaultUserId, TableType.TABLE, true );

        columnPos = 0;
        // Add Columns on Orders Table
        ORDERS_COL_ID_ORDER_ID = catalog.addColumn(
                "order_id", ORDERS_TABLE_ID, columnPos++, PolyType.INTEGER, null, null, null,
                null, null, false, null
        );
        ORDERS_COL_ID_CUSTOMER_ID = catalog.addColumn(
                "customer_id", ORDERS_TABLE_ID, columnPos++, PolyType.INTEGER, null, null, null,
                null, null, false, null
        );
        ORDERS_COL_ID_ORDER_DATE = catalog.addColumn(
                "order_date", ORDERS_TABLE_ID, columnPos++, PolyType.TIMESTAMP, null, null, null,
                null, null, false, null
        );
        ORDERS_COL_ID_HAS_SHIPPED = catalog.addColumn(
                "has_shipped", ORDERS_TABLE_ID, columnPos, PolyType.BOOLEAN, null, null, null,
                null, null, false, null
        );

        try {
            catalog.addPrimaryKey( ORDERS_TABLE_ID, List.of( ORDERS_COL_ID_ORDER_ID ) );
        } catch ( GenericCatalogException e ) {
            e.printStackTrace();
        }

        try {
            catalog.addForeignKey(
                    ORDERS_TABLE_ID, List.of( ORDERS_COL_ID_CUSTOMER_ID ), CUSTOMERS_TABLE_ID, List.of( CUSTOMERS_COL_ID_CUSTOMER_ID ),
                    "order_customer_fk", ForeignKeyOption.NONE, ForeignKeyOption.NONE
            );
        } catch ( GenericCatalogException e ) {
            e.printStackTrace();
        }

        // Products Table
        PRODUCTS_TABLE_ID = catalog.addTable( PRODUCTS_TABLE_NAME, SCHEMA_ID, Catalog.defaultUserId, TableType.TABLE, true );

        columnPos = 0;
        // Add Columns on Products Table
        PRODUCTS_COL_ID_PRODUCT_ID = catalog.addColumn(
                "product_id", PRODUCTS_TABLE_ID, columnPos++, PolyType.INTEGER, null, null, null,
                null, null, false, null
        );
        PRODUCTS_COL_ID_PRODUCT_NAME = catalog.addColumn(
                "product_name", PRODUCTS_TABLE_ID, columnPos++, PolyType.VARCHAR, null, 24, null,
                null, null, false, Collation.CASE_INSENSITIVE
        );
        PRODUCTS_COL_ID_PRODUCT_STOCK = catalog.addColumn(
                "product_stock", PRODUCTS_TABLE_ID, columnPos++, PolyType.INTEGER, null, null, null,
                null, null, false, null
        );
        PRODUCTS_COL_ID_PRODUCT_PRICE = catalog.addColumn(
                "product_price", PRODUCTS_TABLE_ID, columnPos, PolyType.DOUBLE, null, null, null,
                null, null, false, null
        );

        try {
            catalog.addPrimaryKey( PRODUCTS_TABLE_ID, List.of( PRODUCTS_COL_ID_PRODUCT_ID ) );
        } catch ( GenericCatalogException e ) {
            e.printStackTrace();
        }

        // Shipments Table
        SHIPMENTS_TABLE_ID = catalog.addTable( SHIPMENTS_TABLE_NAME, SCHEMA_ID, Catalog.defaultUserId, TableType.TABLE, true );

        columnPos = 0;
        // Add Columns on Shipments Table
        SHIPMENTS_COL_ID_SHIPMENT_ID = catalog.addColumn(
                "shipment_id", SHIPMENTS_TABLE_ID, columnPos++, PolyType.INTEGER, null, null, null,
                null, null, false, null
        );
        SHIPMENTS_COL_ID_ORDER_ID = catalog.addColumn(
                "order_id", SHIPMENTS_TABLE_ID, columnPos++, PolyType.INTEGER, null, null, null,
                null, null, false, null
        );
        SHIPMENTS_COL_ID_SHIPMENT_DATE = catalog.addColumn(
                "shipment_date", SHIPMENTS_TABLE_ID, columnPos, PolyType.DATE, null, null, null,
                null, null, false, null
        );

        try {
            catalog.addPrimaryKey( SHIPMENTS_TABLE_ID, List.of( SHIPMENTS_COL_ID_SHIPMENT_ID ) );
        } catch ( GenericCatalogException e ) {
            e.printStackTrace();
        }

        try {
            catalog.addForeignKey(
                    SHIPMENTS_TABLE_ID, List.of( SHIPMENTS_COL_ID_ORDER_ID ), ORDERS_TABLE_ID, List.of( ORDERS_COL_ID_ORDER_ID ),
                    "shipment_order_fk", ForeignKeyOption.NONE, ForeignKeyOption.NONE
            );
        } catch ( GenericCatalogException e ) {
            e.printStackTrace();
        }

        // Purchases Table
        PURCHASES_TABLE_ID = catalog.addTable( PURCHASES_TABLE_NAME, SCHEMA_ID, Catalog.defaultUserId, TableType.TABLE, true );

        columnPos = 0;
        // Add Columns on Purchases Table
        PURCHASES_COL_ID_PURCHASE_ID = catalog.addColumn(
                "purchase_id", PURCHASES_TABLE_ID, columnPos++, PolyType.INTEGER, null, null, null,
                null, null, false, null
        );
        PURCHASES_COL_ID_ORDER_ID = catalog.addColumn(
                "order_id", PURCHASES_TABLE_ID, columnPos++, PolyType.INTEGER, null, null, null,
                null, null, false, null
        );
        PURCHASES_COL_ID_PRODUCT_ID = catalog.addColumn(
                "product_id", PURCHASES_TABLE_ID, columnPos++, PolyType.INTEGER, null, null, null,
                null, null, false, null
        );
        PURCHASES_COL_ID_QUANTITY = catalog.addColumn(
                "quantity", PURCHASES_TABLE_ID, columnPos, PolyType.INTEGER, null, null, null,
                null, null, false, null
        );

        try {
            catalog.addPrimaryKey( PURCHASES_TABLE_ID, List.of( PURCHASES_COL_ID_PURCHASE_ID ) );
        } catch ( GenericCatalogException e ) {
            e.printStackTrace();
        }

        try {
            catalog.addForeignKey(
                    PURCHASES_TABLE_ID, List.of( PURCHASES_COL_ID_ORDER_ID ), ORDERS_TABLE_ID, List.of( ORDERS_COL_ID_ORDER_ID ),
                    "purchase_order_fk", ForeignKeyOption.NONE, ForeignKeyOption.NONE
            );
            catalog.addForeignKey(
                    PURCHASES_TABLE_ID, List.of( PURCHASES_COL_ID_PRODUCT_ID ), PRODUCTS_TABLE_ID, List.of( PRODUCTS_COL_ID_PRODUCT_ID ),
                    "purchase_product_fk", ForeignKeyOption.NONE, ForeignKeyOption.NONE
            );
        } catch ( GenericCatalogException e ) {
            e.printStackTrace();
        }

        // Data Placements

        try {

            Transaction transaction = transactionManager.startTransaction(
                    catalog.getUser( Catalog.defaultUserId ).name,
                    catalog.getDatabase( Catalog.defaultDatabaseId ).name,
                    false,
                    null
            );

            Adapter hsqldbAdapter = addHsqldbAdapter();
            HSQLDB_ADAPTER_ID = hsqldbAdapter.getAdapterId();

            addDataPlacements(
                    hsqldbAdapter,
                    List.of(
                            CUSTOMERS_TABLE_ID,
                            ORDERS_TABLE_ID,
                            PRODUCTS_TABLE_ID,
                            SHIPMENTS_TABLE_ID,
                            PURCHASES_TABLE_ID
                    ),
                    transaction
            );

        } catch ( UnknownDatabaseException | GenericCatalogException | UnknownUserException | UnknownSchemaException e ) {
            log.error( "Data placement Creation Failed", e );
            return "Failed.";
        }

        return "Done.";

    }


}
