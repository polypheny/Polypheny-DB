/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.algebra.enumerable;


import com.google.common.collect.ImmutableList;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
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
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.ConformanceEnum;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMdDistribution;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPredicateList;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexSimplify;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Conformance;


/**
 * Implementation of {@link Calc} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableCalc extends Calc implements EnumerableAlg {

    /**
     * Creates an EnumerableCalc.
     *
     * Use {@link #create} unless you know what you're doing.
     */
    public EnumerableCalc( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, RexProgram program ) {
        super( cluster, traitSet, input, program );
        assert getConvention() instanceof EnumerableConvention;
        assert !program.containsAggs();
    }


    public static EnumerableCalc create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        RexProgram p = getProgramFromArgs( args, children.get( 0 ), cluster.getRexBuilder() );
        return new EnumerableCalc( cluster, children.get( 0 ).getTraitSet(), children.get( 0 ), p );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 10 );
    }


    /**
     * Creates an EnumerableCalc.
     */
    public static EnumerableCalc create( final AlgNode input, final RexProgram program ) {
        final AlgCluster cluster = input.getCluster();
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet = cluster.traitSet()
                .replace( EnumerableConvention.INSTANCE )
                .replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.calc( mq, input, program ) )
                .replaceIf( AlgDistributionTraitDef.INSTANCE, () -> AlgMdDistribution.calc( mq, input, program ) )
                .replaceIf( ModelTraitDef.INSTANCE, () -> input.getTraitSet().getTrait( ModelTraitDef.INSTANCE ) );
        return new EnumerableCalc( cluster, traitSet, input, program );
    }


    @Override
    public EnumerableCalc copy( AlgTraitSet traitSet, AlgNode child, RexProgram program ) {
        // we do not need to copy program; it is immutable
        return new EnumerableCalc( getCluster(), traitSet, child, program );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableAlg child = (EnumerableAlg) getInput();

        final Result result = implementor.visitChild( this, 0, child, pref );

        final PhysType physType = PhysTypeImpl.of( typeFactory, getTupleType(), JavaTupleFormat.ARRAY );

        // final Enumerable<Employee> inputEnumerable = <<child adapter>>;
        // return new Enumerable<IntString>() {
        //     Enumerator<IntString> enumerator() {
        //         return new Enumerator<IntString>() {
        //             public void reset() {
        // ...
        Type outputJavaType = physType.getJavaTupleType();
        final Type enumeratorType = Types.of( Enumerator.class, outputJavaType );
        Type inputJavaType = result.physType().getJavaTupleType();
        ParameterExpression inputEnumerator = Expressions.parameter( Types.of( Enumerator.class, inputJavaType ), "inputEnumerator" );
        Expression input = RexToLixTranslator.convert( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ), inputJavaType );

        final RexBuilder rexBuilder = getCluster().getRexBuilder();
        final AlgMetadataQuery mq = AlgMetadataQuery.instance();
        final AlgOptPredicateList predicates = mq.getPulledUpPredicates( child );
        final RexSimplify simplify = new RexSimplify( rexBuilder, predicates, RexUtil.EXECUTOR );
        final RexProgram program = this.program.normalize( rexBuilder, simplify );

        BlockStatement moveNextBody;
        if ( program.getCondition() == null ) {
            moveNextBody = Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_MOVE_NEXT.method ) );
        } else {
            final BlockBuilder builder2 = new BlockBuilder();
            Expression condition =
                    RexToLixTranslator.translateCondition(
                            program,
                            typeFactory,
                            builder2,
                            new RexToLixTranslator.InputGetterImpl( input, result.physType() ),
                            implementor.allCorrelateVariables,
                            implementor.getConformance() );
            builder2.add(
                    EnumUtils.ifThen(
                            condition,
                            Expressions.return_( null, Expressions.constant( true ) ) ) );
            moveNextBody =
                    Expressions.block(
                            Expressions.while_( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_MOVE_NEXT.method ), builder2.toBlock() ),
                            Expressions.return_( null, Expressions.constant( false ) ) );
        }

        final BlockBuilder builder3 = new BlockBuilder();
        final Conformance conformance = ConformanceEnum.DEFAULT;
        List<Expression> expressions =
                RexToLixTranslator.translateProjects(
                        program,
                        typeFactory,
                        conformance,
                        builder3,
                        physType,
                        DataContext.ROOT,
                        new RexToLixTranslator.InputGetterImpl( input, result.physType() ),
                        implementor.allCorrelateVariables );
        builder3.add( Expressions.return_( null, physType.record( expressions ) ) );
        BlockStatement currentBody = builder3.toBlock();

        final Expression inputEnumerable = builder.append( builder.newName( "inputEnumerable" + System.nanoTime() ), result.block(), false );
        final Expression body;

        body = Expressions.new_(
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

