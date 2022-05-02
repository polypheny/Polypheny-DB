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

package org.polypheny.db.adaptimizer.randomtrees;

import org.polypheny.db.adaptimizer.randomschema.DefaultTestEnvironment;
import org.polypheny.db.catalog.Catalog;

public class RelRandomTreeTemplates {

    public static RelRandomTreeTemplate getRelRandomTreeTemplate( Catalog catalog ) {
        return new RandomTreeTemplateBuilder( catalog )
                .setSchemaName( DefaultTestEnvironment.SCHEMA_NAME )
                .addTable( DefaultTestEnvironment.customers )
                .addTable( DefaultTestEnvironment.orders )
                .addTable( DefaultTestEnvironment.products  )
                .addTable( DefaultTestEnvironment.shipments )
                .addTable( DefaultTestEnvironment.purchases )
                .addBinaryOperator( "Join", 5 )
                .addBinaryOperator( "Union", 15 )
                .addBinaryOperator( "Intersect", 7 )
                .addBinaryOperator( "Minus", 3 )
                .addUnaryOperator( "Sort", 15 )
                .addUnaryOperator( "Project", 10 )
                .addUnaryOperator( "Filter", 20 )
                .setUnaryProbability( 0.5f )
                .setSeed( 543774 )
                .setMaxHeight( 5 )
                .build();
    }



}
