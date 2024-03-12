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

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.ConditionalStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.linq4j.tree.Types;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.lpg.LpgUnwind;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BuiltInMethod;


public class EnumerableLpgUnwind extends LpgUnwind implements EnumerableAlg {

    /**
     * Creates a {@link EnumerableLpgUnwind}.
     */
    protected EnumerableLpgUnwind( AlgCluster cluster, AlgTraitSet traits, AlgNode input, int index, String alias ) {
        super( cluster, traits, input, index, alias );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 2 );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableLpgUnwind( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), index, alias );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        BlockBuilder builder = new BlockBuilder();
        Result res = implementor.visitChild( this, 0, (EnumerableAlg) input, pref );

        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), pref.prefer( res.format() ) );

        Type outputJavaType = physType.getJavaTupleType();
        final Type enumeratorType = Types.of( Enumerator.class, outputJavaType );
        Type inputJavaType = res.physType().getJavaTupleType();

        ParameterExpression inputEnumerator = Expressions.parameter( Types.of( Enumerator.class, inputJavaType ), "inputEnumerator" );

        Expression inputEnumerable = builder.append( builder.newName( "inputEnumerable" + System.nanoTime() ), res.block(), false );

        final ParameterExpression i_ = Expressions.parameter( int.class, "_i" );
        final ParameterExpression list_ = Expressions.parameter( Types.of( List.class, PolyValue.class ), "_callList" );
        final ParameterExpression unset_ = Expressions.parameter( boolean.class, "_unset" );

        BlockStatement moveNextBody;
        BlockBuilder unwindBlock = new BlockBuilder();

        ConditionalStatement ifNotSetInitial = EnumUtils.ifThen(
                unset_,
                Expressions.block(
                        Expressions.statement( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_MOVE_NEXT.method ) ),
                        assignNextElement( list_, inputEnumerator ),
                        Expressions.statement( Expressions.assign( i_, Expressions.constant( -1 ) ) ),
                        Expressions.statement( Expressions.assign( unset_, Expressions.constant( false ) ) )
                ) );

        unwindBlock.add( ifNotSetInitial );

        unwindBlock.add(
                EnumUtils.ifThenElse(
                        Expressions.lessThan( i_, Expressions.subtract( Expressions.call( list_, "size" ), Expressions.constant( 1 ) ) ),
                        Expressions.block(
                                Expressions.statement(
                                        Expressions.assign(
                                                i_,
                                                Expressions.add(
                                                        i_,
                                                        Expressions.constant( 1 ) ) ) ),
                                Expressions.return_( null, Expressions.constant( true ) ) ),
                        Expressions.block(
                                Expressions.statement( Expressions.assign( unset_, Expressions.constant( true ) ) ),
                                Expressions.statement( Expressions.assign( i_, Expressions.constant( 0 ) ) ),
                                Expressions.return_( null, Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_MOVE_NEXT.method ) ) ) )
        );
        moveNextBody = unwindBlock.toBlock();

        BlockBuilder currentBuilder = new BlockBuilder();

        ConditionalStatement ifNotSet = EnumUtils.ifThen(
                unset_,
                Expressions.block(
                        assignNextElement( list_, inputEnumerator ),
                        Expressions.statement( Expressions.assign( i_, Expressions.constant( 0 ) ) ),
                        Expressions.statement( Expressions.assign( unset_, Expressions.constant( false ) ) )
                ) );

        currentBuilder.add( ifNotSet );
        currentBuilder.add( Expressions.return_( null, Expressions.newArrayInit( PolyValue.class, Expressions.convert_( Expressions.call( list_, "get", i_ ), PolyValue.class ) ) ) );

        BlockStatement currentBody = currentBuilder.toBlock();

        Expression body = Expressions.new_(
                enumeratorType,
                EnumUtils.NO_EXPRS,
                Expressions.list(
                        Expressions.fieldDecl( Modifier.PUBLIC, list_, Expressions.constant( Collections.emptyList() ) ),
                        Expressions.fieldDecl( Modifier.PUBLIC, i_, Expressions.constant( 0 ) ),
                        Expressions.fieldDecl( Modifier.PUBLIC, unset_, Expressions.constant( true ) ),
                        Expressions.fieldDecl(
                                Modifier.PUBLIC | Modifier.FINAL,
                                inputEnumerator,
                                Expressions.call( inputEnumerable, BuiltInMethod.ENUMERABLE_ENUMERATOR.method ) ),
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_RESET.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_RESET.method ) ) ),
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_MOVE_NEXT.method,
                                EnumUtils.NO_PARAMS,
                                moveNextBody ),
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_CLOSE.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CLOSE.method ) ) ),
                        Expressions.methodDecl(
                                Modifier.PUBLIC,
                                EnumUtils.BRIDGE_METHODS
                                        ? Object.class
                                        : outputJavaType,
                                "current",
                                EnumUtils.NO_PARAMS,
                                currentBody ) ) );

        builder.add(
                Expressions.return_(
                        null,
                        Expressions.new_(
                                BuiltInMethod.ABSTRACT_ENUMERABLE_CTOR.constructor,
                                // TODO: generics
                                //   Collections.singletonList(inputRowType),
                                EnumUtils.NO_EXPRS,
                                ImmutableList.<MemberDeclaration>of( Expressions.methodDecl( Modifier.PUBLIC, enumeratorType, BuiltInMethod.ENUMERABLE_ENUMERATOR.method.getName(), EnumUtils.NO_PARAMS, Blocks.toFunctionBlock( body ) ) ) ) ) );
        return implementor.result( physType, builder.toBlock() );
    }


    @NotNull
    private static Statement assignNextElement( ParameterExpression list_, ParameterExpression inputEnumerator ) {
        return Expressions.statement( Expressions.assign( list_,
                Expressions.convert_(
                        Expressions.arrayIndex(
                                Expressions.convert_( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ), Types.of( PolyValue[].class ) ),
                                Expressions.constant( 0 ) ),
                        Types.of( List.class, PolyValue.class ) ) ) );
    }


}
