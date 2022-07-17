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

package org.polypheny.db.adaptimizer.sessions;

import java.util.List;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adaptimizer.AdaptiveOptimizerImpl;
import org.polypheny.db.adaptimizer.rndqueries.AbstractQuerySupplier;
import org.polypheny.db.adaptimizer.rndqueries.QuerySupplier;
import org.polypheny.db.adaptimizer.rndqueries.QueryTemplate;
import org.polypheny.db.adaptimizer.rndqueries.RelQueryGenerator;
import org.polypheny.db.adaptimizer.rndqueries.RelQueryTemplate;
import org.polypheny.db.adaptimizer.rndschema.DefaultTestEnvironment;
import org.polypheny.db.algebra.core.JoinAlgType;

@Slf4j
public abstract class SessionUtil {

    public static QueryTemplate getDefaultRelTreeTemplate() {
        return RelQueryTemplate.builder( AdaptiveOptimizerImpl.getCatalog(), List.of(
                        DefaultTestEnvironment.customers,
                        DefaultTestEnvironment.orders,
                        DefaultTestEnvironment.products,
                        DefaultTestEnvironment.shipments,
                        DefaultTestEnvironment.purchases
                ) )
                .schemaName( DefaultTestEnvironment.SCHEMA_NAME )
                .addBinaryOperator( "Join", 5 )
                .addBinaryOperator( "Union", 15 )
                .addBinaryOperator( "Intersect", 7 )
                .addBinaryOperator( "Minus", 3 )
                .addUnaryOperator( "Sort", 15 )
                .addUnaryOperator( "Project", 10 )
                .addUnaryOperator( "Filter", 20 )
                .addFilterOperator( "<>" )
                .addJoinOperator( "=" )
                .addJoinType( JoinAlgType.FULL )
                .addJoinType( JoinAlgType.INNER )
                .addJoinType( JoinAlgType.LEFT )
                .addJoinType( JoinAlgType.RIGHT )
                .unaryProbability( 0.5f )
                .seed( 3657567 )
                .random( new Random( 3657567 ) ) // Todo fix
                .height( 5 )
                .build();
    }

    public static AbstractQuerySupplier getQueryGenerator( QueryTemplate template ) {
        return new QuerySupplier( new RelQueryGenerator( template, false, true ) );
    }

}
