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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.Getter;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.core.common.Transformer;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


@Getter
public class EnumerableTransformer extends Transformer implements EnumerableAlg {


    /**
     * Creates an {@link EnumerableTransformer}, which is able to switch {@link org.polypheny.db.schema.ModelTraitDef} for
     * non-native underlying adapters if needed.
     * For example, it will transform the {@link org.polypheny.db.algebra.core.lpg.LpgScan}, which can be handled directly by
     * a native adapter, to a combination of {@link Scan} and {@link org.polypheny.db.algebra.core.Union}.
     */
    public EnumerableTransformer( AlgOptCluster cluster, List<AlgNode> inputs, List<String> names, AlgTraitSet traitSet, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType, boolean isCrossModel ) {
        super( cluster, inputs, names, traitSet, inTraitSet, outTraitSet, rowType, isCrossModel );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        if ( outModelTrait == ModelTrait.DOCUMENT ) {

            if ( isCrossModel && inModelTrait == ModelTrait.GRAPH ) {
                return implementDocumentOnGraph( implementor, pref );
            }
            return implementDocument( implementor, pref );
        }

        if ( rowType.getFieldList().stream().map( f -> f.getType().getPolyType() ).noneMatch( t -> t == PolyType.EDGE || t == PolyType.NODE || t == PolyType.GRAPH ) ) {
            return implementUnModifyTransform( implementor, pref );
        }

        if ( outModelTrait == ModelTrait.GRAPH ) {
            if ( isCrossModel ) {
                if ( inModelTrait == ModelTrait.RELATIONAL ) {
                    return implementGraphOnRelational( implementor, pref );
                } else if ( inModelTrait == ModelTrait.DOCUMENT ) {
                    return implementGraphOnDocument( implementor, pref );
                }
            }
            return implementGraph( implementor, pref );
        }

        throw new RuntimeException( "Transformation of the given data models is not yet supported." );
    }


    private Result implementGraphOnDocument( EnumerableAlgImplementor implementor, Prefer pref ) {
        BlockBuilder builder = new BlockBuilder();
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();

        final Map<String, Result> nodes = new HashMap<>();
        for ( int i = 0; i < getInputs().size(); i++ ) {
            nodes.put( names.get( i ), implementor.visitChild( this, i, (EnumerableAlg) getInput( i ), pref ) );
        }

        //final Result edges = implementor.visitChild( this, 1, (EnumerableAlg) getInput( 1 ), pref );

        final PhysType physType = PhysTypeImpl.of( typeFactory, getRowType(), pref.prefer( JavaRowFormat.SCALAR ) );

        Type inputJavaType = physType.getJavaRowType();
        ParameterExpression inputEnumerator = Expressions.parameter( Types.of( Enumerator.class, inputJavaType ), "inputEnumerator" );

        Type outputJavaType = physType.getJavaRowType();
        final Type enumeratorType = Types.of( Enumerator.class, outputJavaType );

        List<Expression> tableAsNodes = new ArrayList<>();
        int i = 0;
        for ( Entry<String, Result> entry : nodes.entrySet() ) {
            Expression exp = builder.append( builder.newName( "nodes_" + System.nanoTime() ), entry.getValue().block );
            MethodCallExpression transformedTable = Expressions.call( BuiltInMethod.X_MODEL_COLLECTION_TO_NODE.method, exp, Expressions.constant( entry.getKey() ) );
            tableAsNodes.add( transformedTable );
            i++;
        }

        Expression nodesExp = Expressions.call( BuiltInMethod.X_MODEL_MERGE_NODE_COLLECTIONS.method, EnumUtils.expressionList( tableAsNodes ) );

        //Expression nodesExp = builder.append( builder.newName( "nodes_" + System.nanoTime() ), nodes.block );
        //Expression edgeExp = builder.append( builder.newName( "edges_" + System.nanoTime() ), edges.block );

        //MethodCallExpression nodeCall = Expressions.call( BuiltInMethod.TO_NODE.method, nodesExp );
        //MethodCallExpression edgeCall = Expressions.call( BuiltInMethod.TO_EDGE.method, edgeExp );

        MethodCallExpression call = Expressions.call( BuiltInMethod.TO_GRAPH.method, nodesExp, Expressions.call( Linq4j.class, "emptyEnumerable" ) );

        Expression body = Expressions.new_(
                enumeratorType,
                EnumUtils.NO_EXPRS,
                Expressions.list(
                        Expressions.fieldDecl(
                                Modifier.PUBLIC | Modifier.FINAL,
                                inputEnumerator,
                                Expressions.call( call, BuiltInMethod.ENUMERABLE_ENUMERATOR.method ) ),
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
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_CURRENT.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ) ) )
                ) );

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


    private Result implementGraphOnRelational( EnumerableAlgImplementor implementor, Prefer pref ) {
        BlockBuilder builder = new BlockBuilder();
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();

        final Map<String, Pair<AlgNode, Result>> nodes = new HashMap<>();
        for ( int i = 0; i < getInputs().size(); i++ ) {
            nodes.put( names.get( i ), Pair.of( getInput( i ), implementor.visitChild( this, i, (EnumerableAlg) getInput( i ), pref ) ) );
        }

        //final Result edges = implementor.visitChild( this, 1, (EnumerableAlg) getInput( 1 ), pref );

        final PhysType physType = PhysTypeImpl.of( typeFactory, getRowType(), pref.prefer( JavaRowFormat.SCALAR ) );

        Type inputJavaType = physType.getJavaRowType();
        ParameterExpression inputEnumerator = Expressions.parameter( Types.of( Enumerator.class, inputJavaType ), "inputEnumerator" );

        Type outputJavaType = physType.getJavaRowType();
        final Type enumeratorType = Types.of( Enumerator.class, outputJavaType );

        List<Expression> tableAsNodes = new ArrayList<>();
        int i = 0;
        for ( Entry<String, Pair<AlgNode, Result>> entry : nodes.entrySet() ) {
            Expression exp = builder.append( builder.newName( "nodes_" + System.nanoTime() ), entry.getValue().right.block );
            MethodCallExpression transformedTable = Expressions.call( BuiltInMethod.X_MODEL_TABLE_TO_NODE.method, exp, Expressions.constant( entry.getKey() ), EnumUtils.constantArrayList( entry.getValue().getKey().getRowType().getFieldNames(), String.class ) );
            tableAsNodes.add( transformedTable );
            i++;
        }

        Expression nodesExp = Expressions.call( BuiltInMethod.X_MODEL_MERGE_NODE_COLLECTIONS.method, EnumUtils.expressionList( tableAsNodes ) );

        MethodCallExpression call = Expressions.call( BuiltInMethod.TO_GRAPH.method, nodesExp, Expressions.call( Linq4j.class, "emptyEnumerable" ) );

        Expression body = Expressions.new_(
                enumeratorType,
                EnumUtils.NO_EXPRS,
                Expressions.list(
                        Expressions.fieldDecl(
                                Modifier.PUBLIC | Modifier.FINAL,
                                inputEnumerator,
                                Expressions.call( call, BuiltInMethod.ENUMERABLE_ENUMERATOR.method ) ),
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
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_CURRENT.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ) ) )
                ) );

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


    private Result implementGraph( EnumerableAlgImplementor implementor, Prefer pref ) {
        BlockBuilder builder = new BlockBuilder();
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();

        final Result nodes = implementor.visitChild( this, 0, (EnumerableAlg) getInput( 0 ), pref );
        final Result edges = implementor.visitChild( this, 1, (EnumerableAlg) getInput( 1 ), pref );

        final PhysType physType = PhysTypeImpl.of( typeFactory, getRowType(), pref.prefer( JavaRowFormat.SCALAR ) );

        //
        Type inputJavaType = physType.getJavaRowType();
        ParameterExpression inputEnumerator = Expressions.parameter( Types.of( Enumerator.class, inputJavaType ), "inputEnumerator" );

        Type outputJavaType = physType.getJavaRowType();
        final Type enumeratorType = Types.of( Enumerator.class, outputJavaType );

        Expression nodesExp = builder.append( builder.newName( "nodes_" + System.nanoTime() ), nodes.block );
        Expression edgeExp = builder.append( builder.newName( "edges_" + System.nanoTime() ), edges.block );

        MethodCallExpression nodeCall = Expressions.call( BuiltInMethod.TO_NODE.method, nodesExp );
        MethodCallExpression edgeCall = Expressions.call( BuiltInMethod.TO_EDGE.method, edgeExp );

        MethodCallExpression call = Expressions.call( BuiltInMethod.TO_GRAPH.method, nodeCall, edgeCall );

        Expression body = Expressions.new_(
                enumeratorType,
                EnumUtils.NO_EXPRS,
                Expressions.list(
                        Expressions.fieldDecl(
                                Modifier.PUBLIC | Modifier.FINAL,
                                inputEnumerator,
                                Expressions.call( call, BuiltInMethod.ENUMERABLE_ENUMERATOR.method ) ),
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
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_CURRENT.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ) ) )
                ) );

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


    private Result implementDocument( EnumerableAlgImplementor implementor, Prefer pref ) {
        return implementor.visitChild( this, 0, (EnumerableAlg) getInput( 0 ), pref );
    }


    private Result implementUnModifyTransform( EnumerableAlgImplementor implementor, Prefer pref ) {
        if ( getInputs().size() == 1 ) {
            return implementor.visitChild( this, 0, (EnumerableAlg) getInputs().get( 0 ), pref );
        }
        throw new UnsupportedOperationException();
    }


    private Result implementDocumentOnGraph( EnumerableAlgImplementor implementor, Prefer pref ) {
        BlockBuilder builder = new BlockBuilder();
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();

        Result res = implementor.visitChild( this, 0, (EnumerableAlg) getInput( 0 ), pref );

        final PhysType physType = PhysTypeImpl.of( typeFactory, getRowType(), pref.prefer( JavaRowFormat.SCALAR ) );

        Type inputJavaType = physType.getJavaRowType();
        ParameterExpression inputEnumerator = Expressions.parameter( Types.of( Enumerator.class, inputJavaType ), "inputEnumerator" );

        Expression nodesExp = builder.append( builder.newName( "nodes_" + System.nanoTime() ), res.block );

        Type outputJavaType = physType.getJavaRowType();
        final Type enumeratorType = Types.of( Enumerator.class, outputJavaType );

        MethodCallExpression call = Expressions.call( BuiltInMethod.X_MODEL_NODE_TO_COLLECTION.method, nodesExp );
        Expression body = Expressions.new_(
                enumeratorType,
                EnumUtils.NO_EXPRS,
                Expressions.list(
                        Expressions.fieldDecl(
                                Modifier.PUBLIC | Modifier.FINAL,
                                inputEnumerator,
                                Expressions.call( call, BuiltInMethod.ENUMERABLE_ENUMERATOR.method ) ),
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
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_CURRENT.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ) ) )
                ) );

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
    public AlgWriter explainTerms( AlgWriter pw ) {
        AlgWriter writer = super.explainTerms( pw );
        int i = 0;
        for ( AlgNode input : getInputs() ) {
            writer.input( "input#" + i, input );
            i++;
        }
        return writer;
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 1.5 );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableTransformer( inputs.get( 0 ).getCluster(), inputs, names, traitSet, inModelTrait, outModelTrait, rowType, isCrossModel );
    }

}
