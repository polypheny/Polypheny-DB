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

package org.polypheny.db.adapter.enumerable;


import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.replication.ModifyDataCapture;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.replication.cdc.ChangeDataCollector;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;


/**
 * Should only be placed on the right side of a streamer.
 * Receives basic operations from one or several TableModifies or ModifyCollects to save into a queue to replicate it lazily
 * to other placements.
 */
public class EnumerableModifyDataCapture extends ModifyDataCapture implements EnumerableAlg {


    public static final Method CAPTURE_DATA_MODIFICATIONS = Types.lookupMethod(
            ChangeDataCollector.class,
            "captureChanges",
            DataContext.class );


    /**
     * Creates an <code>AbstractRelNode</code>.
     */
    public EnumerableModifyDataCapture(
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            Operation operation,
            long tableId,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            RexNode condition,
            List<AlgDataTypeField> fieldList,
            List<Long> accessedPartitions,
            long txId,
            long stmtId ) {
        super( cluster, traitSet, operation, tableId, updateColumnList, sourceExpressionList, condition, fieldList, accessedPartitions, txId, stmtId );
    }


    public static EnumerableModifyDataCapture create(
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            Operation operation,
            long tableId,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            RexNode condition,
            List<AlgDataTypeField> fieldList,
            List<Long> accessedPartitions,
            long txId,
            long stmtId ) {
        return new EnumerableModifyDataCapture( cluster, traitSet, operation, tableId, updateColumnList, sourceExpressionList, condition, fieldList, accessedPartitions, txId, stmtId );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();

        //* Streamer just requires that an Enumerable is returned

        final Expression stmtId =
                builder.append(
                        "stmtId",
                        Expressions.constant( getStmtId() ),
                        false );

        final ParameterExpression dataCaptureParameter = Expressions.parameter( Long.class, builder.newName( "registerDataCapture" ) );

        builder.add(
                Expressions.declare(
                        Modifier.FINAL,
                        dataCaptureParameter,
                        Expressions.call( CAPTURE_DATA_MODIFICATIONS, DataContext.ROOT ) ) );

        final Expression captureParameter =
                builder.append(
                        "capture",
                        dataCaptureParameter,
                        false );

        MethodCallExpression enumWrapper = Expressions.call(
                BuiltInMethod.SINGLETON_ENUMERABLE.method,
                captureParameter );

        builder.add( Expressions.return_( null, builder.append( "capture", enumWrapper ) ) );


        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref == Prefer.ARRAY
                                ? JavaRowFormat.ARRAY
                                : JavaRowFormat.SCALAR );

        return implementor.result( physType, builder.toBlock() );
    }
}
