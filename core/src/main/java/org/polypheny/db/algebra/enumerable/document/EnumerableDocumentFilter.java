/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.algebra.enumerable.document;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.document.DocumentFilter;
import org.polypheny.db.algebra.enumerable.CallImplementor;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.enumerable.RexImpTable;
import org.polypheny.db.algebra.enumerable.RexImpTable.MethodImplementor;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator.InputGetter;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator.InputGetterImpl;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;

public class EnumerableDocumentFilter extends DocumentFilter implements EnumerableAlg {

    /**
     * Creates a {@link DocumentFilter}.
     * {@link ModelTrait#DOCUMENT} native node of a filter.
     *
     * @param cluster
     * @param traits
     * @param input
     * @param condition
     */
    protected EnumerableDocumentFilter( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, RexNode condition ) {
        super( cluster, traits, input, condition );
    }


    @Override
    protected AlgNode copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        return new EnumerableDocumentFilter( input.getCluster(), traitSet, input, condition );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableAlg child = (EnumerableAlg) getInput();

        final Result result = implementor.visitChild( this, 0, child, pref );

        final PhysType physType = PhysTypeImpl.of( typeFactory, getRowType(), pref.prefer( result.format ) );

        // final Enumerable<Employee> inputEnumerable = <<child adapter>>;
        // return new Enumerable<IntString>() {
        //     Enumerator<IntString> enumerator() {
        //         return new Enumerator<IntString>() {
        //             public void reset() {
        // ...
        Type outputJavaType = physType.getJavaRowType();
        final Type enumeratorType = Types.of( Enumerator.class, outputJavaType );
        Type inputJavaType = result.physType.getJavaRowType();
        ParameterExpression inputEnumerator = Expressions.parameter( Types.of( Enumerator.class, inputJavaType ), "inputEnumerator" );

        final BlockBuilder builder2 = new BlockBuilder();
        Expression condition = translateFilter( inputEnumerator, this.condition, builder, result.physType );
        builder2.add(
                Expressions.ifThen(
                        condition,
                        Expressions.return_( null, Expressions.constant( true ) ) ) );
        BlockStatement moveNextBody =
                Expressions.block(
                        Expressions.while_( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_MOVE_NEXT.method ), builder2.toBlock() ),
                        Expressions.return_( null, Expressions.constant( false ) ) );

        final Expression inputEnumerable = builder.append( builder.newName( "inputEnumerable" + System.nanoTime() ), result.block, false );
        final Expression body = Expressions.new_(
                enumeratorType,
                EnumUtils.NO_EXPRS,
                Expressions.list(
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
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ) ) ) ) );

        builder.add(
                Expressions.return_(
                        null,
                        Expressions.new_(
                                BuiltInMethod.ABSTRACT_ENUMERABLE_CTOR.constructor,
                                // TODO: generics
                                //   Collections.singletonList(inputRowType),
                                EnumUtils.NO_EXPRS,
                                ImmutableList.of( Expressions.methodDecl( Modifier.PUBLIC, enumeratorType, BuiltInMethod.ENUMERABLE_ENUMERATOR.method.getName(), EnumUtils.NO_PARAMS, Blocks.toFunctionBlock( body ) ) ) ) ) );
        return implementor.result( physType, builder.toBlock() );
    }


    public static Expression translateFilter( Expression inputEnumerator, RexNode node, BlockBuilder builder, PhysType physType ) {
        InputGetterImpl getter = new RexToLixTranslator.InputGetterImpl( Collections.singletonList( Pair.of( inputEnumerator, physType ) ) );

        return translate( node, getter, builder );
    }


    public static Expression translate( RexNode node, InputGetter getter, BlockBuilder builder ) {
        switch ( node.getKind() ) {
            case INPUT_REF:
                final int index = ((RexInputRef) node).getIndex();
                Expression x = getter.field( builder, index, null );
                return builder.append( "inp" + index + "_", x );
            case LITERAL:
                return translateLiteral( (RexLiteral) node, getter, builder );
            default:
                if ( node instanceof RexCall ) {
                    return translateCall( (RexCall) node, getter, builder );
                }
                throw new RuntimeException( "cannot translate expression " + node );
        }
    }


    private static Expression translateLiteral( RexLiteral node, InputGetter getter, BlockBuilder builder ) {

        switch ( node.getType().getPolyType() ) {

            default:
                return Expressions.constant( node.getValue() );
        }
    }


    private static Expression translateCall( RexCall call, InputGetter getter, BlockBuilder builder ) {
        CallImplementor implementor = RexImpTable.INSTANCE.get( call.op );

        implementor.implement()

        MethodImplementor methodImplementor = null;

        List<Expression> translatedOperands = call.getOperands().stream().map( n -> translate( n, getter, builder ) ).collect( Collectors.toList() );

        return methodImplementor.implement( new JavaTypeFactoryImpl(), call, translatedOperands );
    }

}
