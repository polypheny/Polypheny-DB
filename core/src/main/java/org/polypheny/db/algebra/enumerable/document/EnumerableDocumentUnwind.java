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

package org.polypheny.db.algebra.enumerable.document;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.ConditionalStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.linq4j.tree.Types.ArrayType;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.document.DocumentUnwind;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.functions.MqlFunctions;
import org.polypheny.db.functions.RefactorFunctions;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BuiltInMethod;

public class EnumerableDocumentUnwind extends DocumentUnwind implements EnumerableAlg {


    private MethodCallExpression value;


    public EnumerableDocumentUnwind( AlgCluster cluster, AlgTraitSet traits, AlgNode input, String path ) {
        super( cluster, traits, input, path );
    }


    @Override
    public EnumerableDocumentUnwind copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableDocumentUnwind( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), path );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        BlockBuilder builder = new BlockBuilder();
        Result res = implementor.visitChild( this, 0, (EnumerableAlg) input, pref );

        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), pref.prefer( res.format() ) );

        Type outputJavaType = physType.getJavaTupleType();
        final Type enumeratorType = Types.of( Enumerator.class, outputJavaType );
        Type inputJavaType = res.physType().getJavaTupleType();

        ParameterExpression inputEnumerator = Expressions.parameter( Types.of( Enumerator.class, PolyValue[].class ), "inputEnumerator" );

        Expression inputEnumerable = builder.append( builder.newName( "inputEnumerable" + System.nanoTime() ), res.block(), false );

        final ParameterExpression i_ = Expressions.parameter( int.class, "_i" );
        final ParameterExpression list_ = Expressions.parameter( Types.of( List.class, PolyValue.class ), "_callList" );
        final ParameterExpression unset_ = Expressions.parameter( boolean.class, "_unset" );
        final ParameterExpression doc_ = Expressions.parameter( PolyValue.class, "_doc" );

        BlockStatement moveNextBody;
        BlockBuilder unwindBlock = new BlockBuilder();

        Expression fullCurrent = Expressions.arrayIndex( Expressions.convert_( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ), PolyValue[].class ), Expressions.constant( 0 ) );
        value = Expressions.call(
                BuiltInMethod.MQL_QUERY_VALUE.method,
                Expressions.convert_( doc_, PolyValue.class ),
                PolyList.copyOf( Arrays.stream( path.split( "\\." ) ).map( PolyString::of ).collect( Collectors.toList() ) ).asExpression() );

        value = Expressions.call( MqlFunctions.class, "getAsList", value );

        ConditionalStatement ifNotSetInitial = EnumUtils.ifThen(
                unset_,
                Expressions.block(
                        Expressions.statement( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_MOVE_NEXT.method ) ),
                        /*
                         list = docQueryValue( enumerable.next(), PolyList[]);
                         */
                        Expressions.statement( Expressions.assign( doc_, fullCurrent ) ),
                        Expressions.statement( Expressions.assign( list_, Expressions.convert_( value, Types.of( List.class, PolyValue.class ) ) ) ),
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
                        Expressions.statement( Expressions.assign( doc_, fullCurrent ) ),
                        Expressions.statement( Expressions.assign( list_, Expressions.convert_( value, Types.of( List.class, PolyValue.class ) ) ) ),
                        Expressions.statement( Expressions.assign( i_, Expressions.constant( 0 ) ) ),
                        Expressions.statement( Expressions.assign( unset_, Expressions.constant( false ) ) )
                ) );

        currentBuilder.add( ifNotSet );
        // return mergeDocuments( doc_, Pair(path, list.get(i_)));
        currentBuilder.add(
                Expressions.return_( null,
                        Expressions.newArrayInit( new ArrayType( PolyValue.class ),
                                Expressions.call( RefactorFunctions.class, "mergeDocuments", doc_,
                                        Expressions.call( BuiltInMethod.PAIR_OF.method, Expressions.constant( path ), Expressions.call( list_, "get", i_ ) ) ) ) ) );

        BlockStatement currentBody = currentBuilder.toBlock();

        Expression body = Expressions.new_(
                enumeratorType,
                EnumUtils.NO_EXPRS,
                Expressions.list(
                        Expressions.fieldDecl( Modifier.PUBLIC, list_, Expressions.constant( Collections.emptyList() ) ),
                        Expressions.fieldDecl( Modifier.PUBLIC, i_, Expressions.constant( 0 ) ),
                        Expressions.fieldDecl( Modifier.PUBLIC, unset_, Expressions.constant( true ) ),
                        Expressions.fieldDecl( Modifier.PUBLIC, doc_, Expressions.constant( null ) ),
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

}
