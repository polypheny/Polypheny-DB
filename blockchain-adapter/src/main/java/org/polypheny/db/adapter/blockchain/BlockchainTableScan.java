/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.blockchain;


import java.util.List;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableRel;
import org.polypheny.db.adapter.enumerable.EnumerableRelImplementor;
import org.polypheny.db.adapter.enumerable.PhysType;
import org.polypheny.db.adapter.enumerable.PhysTypeImpl;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelWriter;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;


/**
 * Relational expression representing a scan of a CSV file.
 *
 * Like any table scan, it serves as a leaf node of a query tree.
 */
public class BlockchainTableScan extends TableScan implements EnumerableRel {

    final BlockchainTable blockchainTable;
    final int[] fields;


    protected BlockchainTableScan(RelOptCluster cluster, RelOptTable table, BlockchainTable blockchainTable, int[] fields ) {
        super( cluster, cluster.traitSetOf( EnumerableConvention.INSTANCE ), table );
        this.blockchainTable = blockchainTable;
        this.fields = fields;
        assert blockchainTable != null;
    }


    @Override
    public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        assert inputs.isEmpty();
        return new BlockchainTableScan( getCluster(), table, blockchainTable, fields );
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        return super.explainTerms( pw ).item( "fields", Primitive.asList( fields ) );
    }


    @Override
    public RelDataType deriveRowType() {
        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
        final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        for ( int field : fields ) {
            builder.add( fieldList.get( field ) );
        }
        return builder.build();
    }


    @Override
    public void register( RelOptPlanner planner ) {
        planner.addRule( BlockchainProjectTableScanRule.INSTANCE );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        // Multiply the cost by a factor that makes a scan more attractive if it has significantly fewer fields than the original scan.
        //
        // The "+ 2D" on top and bottom keeps the function fairly smooth.
        //
        // For example, if table has 3 fields, project has 1 field, then factor = (1 + 2) / (3 + 2) = 0.6
        return super.computeSelfCost( planner, mq ).multiplyBy( ((double) fields.length + 2D) / ((double) table.getRowType().getFieldCount() + 2D) );
    }


    @Override
    public Result implement( EnumerableRelImplementor implementor, Prefer pref ) {
        PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getRowType(), pref.preferArray() );

        /*if ( table instanceof JsonTable ) {
            return implementor.result( physType, Blocks.toBlock( Expressions.call( table.getExpression( JsonTable.class ), "enumerable" ) ) );
        }*/
        return implementor.result( physType, Blocks.toBlock( Expressions.call( table.getExpression( BlockchainTable.class ), "project", implementor.getRootExpression(), Expressions.constant( fields ) ) ) );
    }
}

