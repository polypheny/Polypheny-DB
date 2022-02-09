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

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.enumerable.RexToLixTranslator.InputGetterImpl;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Transformer;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.runtime.PolyCollections.PolyMap;
import org.polypheny.db.serialize.PolySerializer;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;

public class EnumerableTransformer extends Transformer implements EnumerableAlg {

    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param input Input relational expression
     */
    protected EnumerableTransformer(
            AlgOptCluster cluster,
            AlgTraitSet traits,
            AlgDataType rowType,
            AlgNode input,
            List<PolyType> unsupportedTypes,
            PolyType substituteType ) {
        super( cluster, traits, rowType, input, unsupportedTypes, substituteType );
    }


    public static EnumerableTransformer create(
            Transformer transformer,
            AlgTraitSet traits,
            AlgNode input ) {
        return new EnumerableTransformer(
                transformer.getCluster(),
                traits,
                transformer.getRowType(),
                input,
                transformer.getUnsupportedTypes(),
                transformer.getSubstituteType() );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        Result orig = implementor.visitChild( this, 0, (EnumerableAlg) input, pref );

        Expression childExp = builder.append( builder.newName( "child_" + System.nanoTime() ), orig.block );

        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getRowType(), pref.prefer( orig.format ) );
        Type inputJavaType = orig.physType.getJavaRowType();
        ParameterExpression inputEnumerator = Expressions.parameter( Types.of( Enumerator.class, inputJavaType ), "inputEnumerator" );

        Type outputJavaType = physType.getJavaRowType();
        final Type enumeratorType = Types.of( Enumerator.class, outputJavaType );

        Expression input = RexToLixTranslator.convert( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ), inputJavaType );

        List<Integer> replacedIndexes = getReplacedFields( getRowType() );

        InputGetterImpl getter = new InputGetterImpl( Collections.singletonList( Pair.of( input, orig.physType ) ) );

        final BlockBuilder builder2 = new BlockBuilder();

        // transform the rows with the substituted entries back to the needed format
        List<Expression> expressions = new ArrayList<>();
        for ( AlgDataTypeField field : getRowType().getFieldList() ) {

            Expression exp = getter.field( builder2, field.getIndex(), null );
            if ( replacedIndexes.contains( field.getIndex() ) ) {
                exp = Expressions.call( PolySerializer.class, "deserializeAndCompress", exp, Expressions.constant( PolyMap.class ) );
            }
            expressions.add( exp );
        }

        builder2.add( Expressions.return_( null, physType.record( expressions ) ) );
        BlockStatement currentBody = builder2.toBlock();

        Expression body = Expressions.new_(
                enumeratorType,
                EnumUtils.NO_EXPRS,
                Expressions.list(
                        Expressions.fieldDecl(
                                Modifier.PUBLIC | Modifier.FINAL,
                                inputEnumerator,
                                Expressions.call( childExp, BuiltInMethod.ENUMERABLE_ENUMERATOR.method ) ),
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_RESET.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_RESET.method ) ) ),
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_MOVE_NEXT.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_MOVE_NEXT.method ) ) ),
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

        //Expression call = Expressions.call( PolySerializer.class, "deserializeEnumerable", childExp );

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


    @Override
    public EnumerableTransformer copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableTransformer( inputs.get( 0 ).getCluster(), traitSet, getRowType(), inputs.get( 0 ), getUnsupportedTypes(), getSubstituteType() );
    }

}
