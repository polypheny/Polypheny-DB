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

package org.polypheny.db.algebra.enumerable;


import java.util.Optional;
import java.util.function.Predicate;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelTableFunctionScan;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that converts a {@link LogicalRelTableFunctionScan} relational expression {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableScanRule extends ConverterRule {

    /**
     * Creates an EnumerableScanRule.
     *
     * @param algBuilderFactory Builder for relational expressions
     */
    public EnumerableScanRule( AlgBuilderFactory algBuilderFactory ) {
        super( LogicalRelScan.class, (Predicate<AlgNode>) r -> true, Convention.NONE, EnumerableConvention.INSTANCE, algBuilderFactory, "EnumerableScanRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        LogicalRelScan scan = (LogicalRelScan) alg;
        Optional<LogicalTable> oEntity = scan.getEntity().unwrap( LogicalTable.class );
        if ( oEntity.isEmpty() ) {
            return null;
        }

        if ( !EnumerableScan.canHandle( oEntity.get() ) ) {
            return null;
        }
        final Expression expression = oEntity.get().asExpression();
        if ( expression == null ) {
            return null;
        }
        return EnumerableScan.create( scan.getCluster(), oEntity.get() );
    }

}

