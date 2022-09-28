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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.adapter.csv;


import java.util.List;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.adapter.enumerable.EnumerableAlg;
import org.polypheny.db.adapter.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.PhysType;
import org.polypheny.db.adapter.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Relational expression representing a scan of a CSV file.
 *
 * Like any table scan, it serves as a leaf node of a query tree.
 */
public class CsvScan extends Scan implements EnumerableAlg {

    final CsvTranslatableTable csvTable;
    final int[] fields;


    protected CsvScan( AlgOptCluster cluster, AlgOptTable table, CsvTranslatableTable csvTable, int[] fields ) {
        super( cluster, cluster.traitSetOf( EnumerableConvention.INSTANCE ), table );
        this.csvTable = csvTable;
        this.fields = fields;

        assert csvTable != null;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert inputs.isEmpty();
        return new CsvScan( getCluster(), table, csvTable, fields );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "fields", Primitive.asList( fields ) );
    }


    @Override
    public AlgDataType deriveRowType() {
        final List<AlgDataTypeField> fieldList = table.getRowType().getFieldList();
        final AlgDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();
        for ( int field : fields ) {
            builder.add( fieldList.get( field ) );
        }
        return builder.build();
    }


    @Override
    public void register( AlgOptPlanner planner ) {
        planner.addRule( CsvProjectScanRule.INSTANCE );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        // Multiply the cost by a factor that makes a scan more attractive if it has significantly fewer fields than the original scan.
        //
        // The "+ 2D" on top and bottom keeps the function fairly smooth.
        //
        // For example, if table has 3 fields, project has 1 field, then factor = (1 + 2) / (3 + 2) = 0.6
        return super.computeSelfCost( planner, mq ).multiplyBy( ((double) fields.length + 2D) / ((double) table.getRowType().getFieldCount() + 2D) );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getRowType(), pref.preferArray() );

        /*if ( table instanceof JsonTable ) {
            return implementor.result( physType, Blocks.toBlock( Expressions.call( table.getExpression( JsonTable.class ), "enumerable" ) ) );
        }*/
        return implementor.result( physType, Blocks.toBlock( Expressions.call( table.getExpression( CsvTranslatableTable.class ), "project", implementor.getRootExpression(), Expressions.constant( fields ) ) ) );
    }

}

