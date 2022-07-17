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

package org.polypheny.db.adaptimizer.rndschema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adaptimizer.AdaptiveOptimizerImpl;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.Catalog.ForeignKeyOption;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.PolyType;


/**
 * Utility Class to create the default test environment for the adaptimizer module.
 */
@Slf4j
public class DefaultTestEnvironment {

    private static AdapterManager adapterManager;
    private static DdlManager ddlManager;

    public static final String SCHEMA_NAME = "adapt";

    // Table IDs
    public static CatalogTable customers;
    public static CatalogTable orders;
    public static CatalogTable products;
    public static CatalogTable shipments;
    public static CatalogTable purchases;


    public static String generate() {
        Catalog catalog = Catalog.getInstance();
        adapterManager = AdapterManager.getInstance();
        ddlManager = DdlManager.getInstance();

        List<DataStore> stores = List.of(
                SchemaUtil.addHsqldbAdapter()
                // SchemaUtil.addPostgreSQLAdapter()
        );

        Transaction transaction;
        try {
            transaction = AdaptiveOptimizerImpl.getTransactionManager().startTransaction( catalog.getUser( Catalog.defaultUserId ).name, catalog.getDatabase( Catalog.defaultDatabaseId ).name, false, "DefaultTestEnvironment-Adaptimizer" );
        } catch ( Exception e ) {
            e.printStackTrace();
            return "Could not create transaction";
        }

        try {

            ddlManager.createSchema( SCHEMA_NAME, Catalog.defaultDatabaseId, SchemaType.RELATIONAL, Catalog.defaultUserId, true, false );
            CatalogSchema catalogSchema = catalog.getSchema( Catalog.defaultDatabaseId, SCHEMA_NAME );

            new TableBuilder( catalogSchema.id, "customers", stores, PlacementType.MANUAL, transaction.createStatement() )
                    .addColumn( "customer_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "customer_name", PolyType.VARCHAR, null, 24, null, null, null, false, Collation.CASE_INSENSITIVE, null )
                    .addColumn( "customer_address", PolyType.VARCHAR, null, 24, null, null, null, false, Collation.CASE_INSENSITIVE, null )
                    .addColumn( "customer_phone", PolyType.VARCHAR, null, 24, null, null, null, false, Collation.CASE_INSENSITIVE, null )
                    .addConstraint( "customers_pk", ConstraintType.PRIMARY, List.of( "customer_id" ) )
                    .build( ddlManager );

            new TableBuilder( catalogSchema.id, "orders", stores, PlacementType.MANUAL, transaction.createStatement() )
                    .addColumn( "order_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "customer_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "order_date", PolyType.TIMESTAMP, null, null, null, null, null, false, null, null )
                    .addColumn( "has_shipped", PolyType.BOOLEAN, null, null, null, null, null, false, null, null )
                    .addConstraint( "orders_pk", ConstraintType.PRIMARY, List.of( "order_id" ) )
                    .build( ddlManager );

            new TableBuilder( catalogSchema.id, "products", stores, PlacementType.MANUAL, transaction.createStatement() )
                    .addColumn( "product_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "product_name", PolyType.VARCHAR, null, 24, null, null, null, false, Collation.CASE_INSENSITIVE, null )
                    .addColumn( "product_stock", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "product_price", PolyType.DOUBLE, null, null, null, null, null, false, null, null )
                    .addConstraint( "products_pk", ConstraintType.PRIMARY, List.of( "product_id" ) )
                    .build( ddlManager );

            new TableBuilder( catalogSchema.id, "shipments", stores, PlacementType.MANUAL, transaction.createStatement() )
                    .addColumn( "shipment_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "order_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "shipment_date", PolyType.DATE, null, null, null, null, null, false, null, null )
                    .addConstraint( "shipments_pk", ConstraintType.PRIMARY, List.of( "shipment_id" ) )
                    .build( ddlManager );

            new TableBuilder( catalogSchema.id, "purchases", stores, PlacementType.MANUAL, transaction.createStatement() )
                    .addColumn( "purchase_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "order_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "product_id", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addColumn( "quantity", PolyType.INTEGER, null, null, null, null, null, false, null, null )
                    .addConstraint( "purchases_pk", ConstraintType.PRIMARY, List.of( "purchase_id" ) )
                    .build( ddlManager );

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
