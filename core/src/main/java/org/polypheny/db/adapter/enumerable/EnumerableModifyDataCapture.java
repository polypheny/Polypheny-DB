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
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.core.TableModify.Operation;
import org.polypheny.db.algebra.replication.ModifyDataCapture;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.replication.ReplicationEngine;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;


public class EnumerableModifyDataCapture extends ModifyDataCapture implements EnumerableAlg {


    public static final Method REGISTER_MODIFY_DATA_CAPTURE = Types.lookupMethod(
            ReplicationEngine.class,
            "registerDataCapture",
            DataContext.class );

    /*
    public static final Method JDBC_SCHEMA_GET_CONNECTION_HANDLER_METHOD = Types.lookupMethod(
            JdbcSchema.class,
            "registerDataCapture",
            DataContext.class );
     */


    /**
     * Creates an <code>AbstractRelNode</code>.
     */
    public EnumerableModifyDataCapture(
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            Operation operation,
            AlgOptTable table,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            List<AlgDataTypeField> fieldList ) {
        super( cluster, traitSet, operation, table, updateColumnList, sourceExpressionList, fieldList );
    }


    public static EnumerableModifyDataCapture create(
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            Operation operation,
            AlgOptTable table,
            List<String> updateColumnList,
            List<RexNode> sourceExpressionList,
            List<AlgDataTypeField> fieldList ) {
        return new EnumerableModifyDataCapture( cluster, traitSet, operation, table, updateColumnList, sourceExpressionList, fieldList );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        /*final BlockBuilder builder = new BlockBuilder();

        final ParameterExpression print_ = Expressions.parameter(String.class, "print");
        final ConstantExpression outValue = Expressions.constant( "Hallo" );

        //System.out.println("==========\n\n\n ========== INSIDE REPLICATOR ==========\n\n\n==========");




        final PhysType physType = PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref == Prefer.ARRAY
                                ? JavaRowFormat.ARRAY
                                : JavaRowFormat.SCALAR );

        builder.add( Expressions.declare( Modifier.FINAL,print_, outValue ));


        builder.add( Expressions.call(
                                Expressions.field(null, System.class, "out"),
                                "println",
                                print_));


        */

        final BlockBuilder builder = new BlockBuilder();

        final Expression countParameter =
                builder.append(
                        "count",
                        Expressions.constant( 2L ),
                        false );

        final Expression updatedCountParameter =
                builder.append(
                        "updatedCount",
                        Expressions.constant( 407L ),
                        false );


/*
        builder.add(
                Expressions.return_(
                        null,
                        Expressions.call(
                                BuiltInMethod.SINGLETON_ENUMERABLE.method,
                                Expressions.convert_(
                                        Expressions.condition(
                                                Expressions.greaterThanOrEqual( updatedCountParameter, countParameter ),
                                                Expressions.subtract( updatedCountParameter, countParameter ),
                                                Expressions.subtract( countParameter, updatedCountParameter ) ),
                                        long.class ) ) ) );
*/

        //* Streamer just requires that an ENumerable is returned

        final BlockBuilder builder2 = new BlockBuilder();
        final ParameterExpression print_ = Expressions.parameter( String.class, "print" );
        final ConstantExpression outValue = Expressions.constant( "Hallo" );

        builder.add( Expressions.declare( Modifier.FINAL, print_, outValue ) );

        MethodCallExpression out = Expressions.call(
                Expressions.field( null, System.class, "out" ),
                "println",
                print_ );


/*

        FunctionExpression expr = Expressions.lambda( Function1.class,
                Expressions.block(
                        Expressions.statement( Expressions.call(
                                Expressions.field(null, System.class, "out"),
                                "println",
                                print_)),
                        Expressions.return_( null,  enumWrapper ) ) );

*/

        final ParameterExpression dataCaptureParameter = Expressions.parameter( Long.class, builder.newName( "registerDataCapture" ) );
        //final Expression enumerable = builder.append( "dataCapture", Expressions.call( REGISTER_MODIFY_DATA_CAPTURE, DataContext.ROOT ) );

        builder.add(
                Expressions.declare(
                        Modifier.FINAL,
                        dataCaptureParameter,
                        Expressions.call( REGISTER_MODIFY_DATA_CAPTURE, DataContext.ROOT ) ) );

        final Expression captureParameter =
                builder.append(
                        "capture",
                        dataCaptureParameter,
                        false );

        MethodCallExpression enumWrapper = Expressions.call(
                BuiltInMethod.SINGLETON_ENUMERABLE.method,
                captureParameter );

        builder.add( Expressions.return_( null, builder.append( "acht", enumWrapper ) ) );

        /*builder.add(
                Expressions.return_( null,
                    Expressions.call( BuiltInMethod.EMPTY_ENUMERABLE.method )) );
*/
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
