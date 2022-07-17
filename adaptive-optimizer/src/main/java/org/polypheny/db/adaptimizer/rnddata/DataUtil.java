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

package org.polypheny.db.adaptimizer.rnddata;

import java.util.List;
import org.polypheny.db.adaptimizer.AdaptiveOptimizerImpl;
import org.polypheny.db.adaptimizer.rndschema.DefaultTestEnvironment;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;

public abstract class DataUtil {

    public static void generateDataForDefaultEnvironment() throws UnknownColumnException {
        new DataGeneratorBuilder( 43268289064L, 100 )
                .addCatalogTable( DefaultTestEnvironment.customers, 1000 )
                .addLengthOption( AdaptiveOptimizerImpl.getCatalog().getColumn( DefaultTestEnvironment.customers.id, "customer_name" ), 8 )
                .addLengthOption( AdaptiveOptimizerImpl.getCatalog().getColumn( DefaultTestEnvironment.customers.id, "customer_phone" ), 10 )
                .addLengthOption( AdaptiveOptimizerImpl.getCatalog().getColumn( DefaultTestEnvironment.customers.id, "customer_address" ), 20 )
                .addCatalogTable( DefaultTestEnvironment.orders, 2000 )
                .addTimestampRangeOption(  AdaptiveOptimizerImpl.getCatalog().getColumn( DefaultTestEnvironment.orders.id, "order_date" ), "2022-05-15 06:00:00", "2023-05-15 06:00:00")
                .addCatalogTable( DefaultTestEnvironment.products, 100 )
                .addUniformValues( AdaptiveOptimizerImpl.getCatalog().getColumn( DefaultTestEnvironment.products.id, "product_name" ), List.of( "Chevrolet", "Hummer", "Fiat", "Peugot", "Farrari", "Nissan", "Porsche") )
                .addIntRangeOption( AdaptiveOptimizerImpl.getCatalog().getColumn( DefaultTestEnvironment.products.id, "product_stock" ), 0, 100 )
                .addDoubleRangeOption( AdaptiveOptimizerImpl.getCatalog().getColumn( DefaultTestEnvironment.products.id, "product_price" ), 5000, 1000000 )
                .addCatalogTable( DefaultTestEnvironment.shipments, 500 )
                .addDateRangeOption( AdaptiveOptimizerImpl.getCatalog().getColumn( DefaultTestEnvironment.shipments.id, "shipment_date" ), "2022-05-15", "2023-05-15" )
                .addCatalogTable( DefaultTestEnvironment.purchases, 20000 )
                .addIntRangeOption( AdaptiveOptimizerImpl.getCatalog().getColumn( DefaultTestEnvironment.purchases.id, "quantity" ), 1, 10 )
                .build().generateData();
    }


}
