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

package org.polypheny.db.adapter.googlesheet;

import java.util.List;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Relational expression representing a relScan of a Google Sheet.
 *
 * Like any table relScan, it serves as a leaf node of a query tree.
 */
public class GoogleSheetTableScanProject extends RelScan<GoogleSheetTable> implements EnumerableAlg {

    final GoogleSheetTable googleSheetTable;
    final int[] fields;


    protected GoogleSheetTableScanProject( AlgCluster cluster, GoogleSheetTable table, GoogleSheetTable googleSheetTable, int[] fields ) {
        super( cluster, cluster.traitSetOf( EnumerableConvention.INSTANCE ), table );
        this.googleSheetTable = googleSheetTable;
        this.fields = fields;

        assert googleSheetTable != null;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert inputs.isEmpty();
        return new GoogleSheetTableScanProject( getCluster(), entity, googleSheetTable, fields );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "fields", Primitive.asList( fields ) );
    }


    @Override
    public AlgDataType deriveRowType() {
        final List<AlgDataTypeField> fieldList = entity.getTupleType().getFields();
        final AlgDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        for ( int field : fields ) {
            builder.add( fieldList.get( field ) );
        }
        return builder.build();
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( ((double) fields.length + 2D) / ((double) entity.getTupleType().getFieldCount() + 2D) );
    }


    @Override
    public void register( AlgPlanner planner ) {
        planner.addRule( GoogleSheetProjectTableScanRule.INSTANCE );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), pref.preferArray() );

        return implementor.result( physType, Blocks.toBlock( Expressions.call( entity.getExpression(), "project", implementor.getRootExpression(), Expressions.constant( fields ) ) ) );
    }

}
