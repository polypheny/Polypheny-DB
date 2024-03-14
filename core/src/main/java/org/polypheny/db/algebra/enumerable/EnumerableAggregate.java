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


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.enumerable.impl.AggAddContextImpl;
import org.polypheny.db.algebra.enumerable.impl.AggResultContextImpl;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.JavaTypeFactoryImpl.SyntheticRecordType;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Implementation of {@link org.polypheny.db.algebra.core.Aggregate} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableAggregate extends Aggregate implements EnumerableAlg {


    public EnumerableAggregate( AlgCluster cluster, AlgTraitSet traitSet, AlgNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) throws InvalidAlgException {
        super( cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls );
        Preconditions.checkArgument( !indicator, "EnumerableAggregate no longer supports indicator fields" );
        assert getConvention() instanceof EnumerableConvention;

        for ( AggregateCall aggCall : aggCalls ) {
            if ( aggCall.isDistinct() ) {
                throw new InvalidAlgException( "distinct aggregation not supported" );
            }
            AggImplementor implementor2 = RexImpTable.INSTANCE.get( aggCall.getAggregation(), false );
            if ( implementor2 == null ) {
                throw new InvalidAlgException( "aggregation " + aggCall.getAggregation() + " not supported" );
            }
        }
    }


    @Override
    public EnumerableAggregate copy( AlgTraitSet traitSet, AlgNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls ) {
        try {
            return new EnumerableAggregate( getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls );
        } catch ( InvalidAlgException e ) {
            // Semantic error not possible. Must be a bug. Convert to internal error.
            throw new AssertionError( e );
        }
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableAlg child = (EnumerableAlg) getInput();
        final Result result = implementor.visitChild( this, 0, child, pref );
        Expression childExp = builder.append( "child", result.block() );

        final PhysType physType = PhysTypeImpl.of( typeFactory, getTupleType(), pref.preferCustom() );

        // final Enumerable<Employee> child = <<child adapter>>;
        // Function1<Employee, Integer> keySelector =
        //     new Function1<Employee, Integer>() {
        //         public Integer apply(Employee a0) {
        //             return a0.deptno;
        //         }
        //     };
        // Function1<Employee, Object[]> accumulatorInitializer =
        //     new Function1<Employee, Object[]>() {
        //         public Object[] apply(Employee a0) {
        //             return new Object[] {0, 0};
        //         }
        //     };
        // Function2<Object[], Employee, Object[]> accumulatorAdder =
        //     new Function2<Object[], Employee, Object[]>() {
        //         public Object[] apply(Object[] a1, Employee a0) {
        //              a1[0] = ((Integer) a1[0]) + 1;
        //              a1[1] = ((Integer) a1[1]) + a0.salary;
        //             return a1;
        //         }
        //     };
        // Function2<Integer, Object[], Object[]> resultSelector =
        //     new Function2<Integer, Object[], Object[]>() {
        //         public Object[] apply(Integer a0, Object[] a1) {
        //             return new Object[] { a0, a1[0], a1[1] };
        //         }
        //     };
        // return childEnumerable
        //     .groupBy(
        //        keySelector, accumulatorInitializer, accumulatorAdder,
        //        resultSelector);
        //
        // or, if key has 0 columns,
        //
        // return childEnumerable
        //     .aggregate(
        //       accumulatorInitializer.apply(),
        //       accumulatorAdder,
        //       resultSelector);
        //
        // with a slightly different resultSelector; or if there are no aggregate
        // functions
        //
        // final Enumerable<Employee> child = <<child adapter>>;
        // Function1<Employee, Integer> keySelector =
        //     new Function1<Employee, Integer>() {
        //         public Integer apply(Employee a0) {
        //             return a0.deptno;
        //         }
        //     };
        // EqualityComparer<Employee> equalityComparer =
        //     new EqualityComparer<Employee>() {
        //         boolean equal(Employee a0, Employee a1) {
        //             return a0.deptno;
        //         }
        //     };
        // return child
        //     .distinct(equalityComparer);

        final PhysType inputPhysType = result.physType();

        ParameterExpression parameter = Expressions.parameter( inputPhysType.getJavaTupleType(), "a0" );

        final PhysType keyPhysType = inputPhysType.project( groupSet.asList(), getGroupType() != Group.SIMPLE, JavaTupleFormat.LIST );
        final int groupCount = getGroupCount();

        final List<AggImpState> aggs = new ArrayList<>( aggCalls.size() );
        for ( Ord<AggregateCall> call : Ord.zip( aggCalls ) ) {
            aggs.add( new AggImpState( call.i, call.e, false ) );
        }

        // Function0<Object[]> accumulatorInitializer =
        //     new Function0<Object[]>() {
        //         public Object[] apply() {
        //             return new Object[] {0, 0};
        //         }
        //     };
        final List<Expression> initExpressions = new ArrayList<>();
        final BlockBuilder initBlock = new BlockBuilder();

        final List<Type> aggStateTypes = new ArrayList<>();
        for ( final AggImpState agg : aggs ) {
            agg.context = new AggContextImpl( agg, typeFactory );
            final List<Type> state = agg.implementor.getStateType( agg.context );

            if ( state.isEmpty() ) {
                agg.state = ImmutableList.of();
                continue;
            }

            aggStateTypes.addAll( state );

            final List<Expression> decls = new ArrayList<>( state.size() );
            for ( int i = 0; i < state.size(); i++ ) {
                String aggName = "a" + agg.aggIdx;
                if ( RuntimeConfig.DEBUG.getBoolean() ) {
                    aggName = Util.toJavaId( agg.call.getAggregation().getName(), 0 ).substring( "ID$0$".length() ) + aggName;
                }
                Type type = state.get( i );
                ParameterExpression pe = Expressions.parameter( type, initBlock.newName( aggName + "s" + i ) );
                initBlock.add( Expressions.declare( 0, pe, null ) );
                decls.add( pe );
            }
            agg.state = decls;
            initExpressions.addAll( decls );
            agg.implementor.implementReset( agg.context, new AggResultContextImpl( initBlock, agg.call, decls, null, null ) );
        }

        final PhysType accPhysType = PhysTypeImpl.of( typeFactory, typeFactory.createSyntheticType( aggStateTypes ) );

        declareParentAccumulator( initExpressions, initBlock, accPhysType );

        final Expression accumulatorInitializer = builder.append( "accumulatorInitializer", Expressions.lambda( Function0.class, initBlock.toBlock() ) );

        // Function2<Object[], Employee, Object[]> accumulatorAdder =
        //     new Function2<Object[], Employee, Object[]>() {
        //         public Object[] apply(Object[] acc, Employee in) {
        //              acc[0] = ((Integer) acc[0]) + 1;
        //              acc[1] = ((Integer) acc[1]) + in.salary;
        //             return acc;
        //         }
        //     };
        final ParameterExpression inParameter = Expressions.parameter( inputPhysType.getJavaTupleType(), "in" );
        final ParameterExpression acc_ = Expressions.parameter( accPhysType.getJavaTupleType(), "acc" );
        for ( int i = 0, stateOffset = 0; i < aggs.size(); i++ ) {
            final BlockBuilder builder2 = new BlockBuilder();
            final AggImpState agg = aggs.get( i );

            final int stateSize = agg.state.size();
            final List<Expression> accumulator = new ArrayList<>( stateSize );
            for ( int j = 0; j < stateSize; j++ ) {
                accumulator.add( accPhysType.fieldReference( acc_, j + stateOffset ) );
            }
            agg.state = accumulator;

            stateOffset += stateSize;

            AggAddContext addContext =
                    new AggAddContextImpl( builder2, accumulator ) {
                        @Override
                        public List<RexNode> rexArguments() {
                            List<AlgDataTypeField> inputTypes = inputPhysType.getTupleType().getFields();
                            List<RexNode> args = new ArrayList<>();
                            for ( int index : agg.call.getArgList() ) {
                                args.add( RexIndexRef.of( index, inputTypes ) );
                            }
                            return args;
                        }


                        @Override
                        public RexNode rexFilterArgument() {
                            return agg.call.filterArg < 0
                                    ? null
                                    : RexIndexRef.of( agg.call.filterArg, inputPhysType.getTupleType() );
                        }


                        @Override
                        public RexToLixTranslator rowTranslator() {
                            return RexToLixTranslator.forAggregation(
                                    typeFactory,
                                    currentBlock(),
                                    new RexToLixTranslator.InputGetterImpl( Collections.singletonList( Pair.of( inParameter, inputPhysType ) ) ),
                                    implementor.getConformance() ).setNullable( currentNullables() );
                        }
                    };

            agg.implementor.implementAdd( agg.context, addContext );
            builder2.add( acc_ );
            agg.accumulatorAdder = builder.append( "accumulatorAdder", Expressions.lambda( Function2.class, builder2.toBlock(), acc_, inParameter ) );
        }

        final ParameterExpression lambdaFactory = Expressions.parameter( AggregateLambdaFactory.class, builder.newName( "lambdaFactory" ) );

        implementLambdaFactory( builder, inputPhysType, aggs, accumulatorInitializer, hasOrderedCall( aggs ), lambdaFactory );
        // Function2<Integer, Object[], Object[]> resultSelector =
        //     new Function2<Integer, Object[], Object[]>() {
        //         public Object[] apply(Integer key, Object[] acc) {
        //             return new Object[] { key, acc[0], acc[1] };
        //         }
        //     };
        final BlockBuilder resultBlock = new BlockBuilder();
        final List<Expression> results = Expressions.list();
        final ParameterExpression key_;
        if ( groupCount == 0 ) {
            key_ = null;
        } else {
            final Type keyType = keyPhysType.getJavaTupleType();
            key_ = Expressions.parameter( keyType, "key" );
            for ( int j = 0; j < groupCount; j++ ) {
                final Expression ref = keyPhysType.fieldReference( key_, j );
                if ( getGroupType() == Group.SIMPLE ) {
                    results.add( ref );
                } else {
                    results.add( EnumUtils.condition( keyPhysType.fieldReference( key_, groupCount + j ), Expressions.constant( null ), Expressions.box( ref ) ) );
                }
            }
        }
        for ( final AggImpState agg : aggs ) {
            results.add( agg.implementor.implementResult( agg.context, new AggResultContextImpl( resultBlock, agg.call, agg.state, key_, keyPhysType ) ) );
        }
        resultBlock.add( physType.record( results ) );
        if ( getGroupType() != Group.SIMPLE ) {
            final List<Expression> list = new ArrayList<>();
            for ( ImmutableBitSet set : groupSets ) {
                list.add( inputPhysType.generateSelector( parameter, groupSet.asList(), set.asList(), keyPhysType.getFormat() ) );
            }
            final Expression keySelectors_ = builder.append( "keySelectors", Expressions.call( BuiltInMethod.ARRAYS_AS_LIST.method, list ) );
            final Expression resultSelector = builder.append( "resultSelector", Expressions.lambda( Function2.class, resultBlock.toBlock(), key_, acc_ ) );
            builder.add(
                    Expressions.return_(
                            null,
                            Expressions.call(
                                    BuiltInMethod.GROUP_BY_MULTIPLE.method,
                                    Expressions.list(
                                            childExp,
                                            keySelectors_,
                                            Expressions.call( lambdaFactory, BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_INITIALIZER.method ),
                                            Expressions.call( lambdaFactory, BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_ADDER.method ),
                                            Expressions.call( lambdaFactory, BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_RESULT_SELECTOR.method, resultSelector ) ).appendIfNotNull( keyPhysType.comparer() ) ) ) );
        } else if ( groupCount == 0 ) {
            final Expression resultSelector = builder.append( "resultSelector", Expressions.lambda( Function1.class, resultBlock.toBlock(), acc_ ) );
            builder.add(
                    Expressions.return_(
                            null,
                            Expressions.call(
                                    BuiltInMethod.SINGLETON_ARRAY_ENUMERABLE.method,
                                    Expressions.call(
                                            childExp,
                                            BuiltInMethod.AGGREGATE.method,
                                            Expressions.call( Expressions.call( lambdaFactory, BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_INITIALIZER.method ), BuiltInMethod.FUNCTION0_APPLY.method ),
                                            Expressions.call( lambdaFactory, BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_ADDER.method ),
                                            Expressions.call( lambdaFactory, BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_SINGLE_GROUP_RESULT_SELECTOR.method, resultSelector ) ) ) ) );
        } else if ( aggCalls.isEmpty() && groupSet.equals( ImmutableBitSet.range( child.getTupleType().getFieldCount() ) ) ) {
            builder.add(
                    Expressions.return_(
                            null,
                            Expressions.call(
                                    inputPhysType.convertTo( childExp, physType ),
                                    BuiltInMethod.DISTINCT.method,
                                    Expressions.<Expression>list().appendIfNotNull( physType.comparer() ) ) ) );
        } else {
            final Expression keySelector_ = builder.append( "keySelector", inputPhysType.generateSelector( parameter, groupSet.asList(), keyPhysType.getFormat() ) );
            final Expression resultSelector_ = builder.append( "resultSelector", Expressions.lambda( Function2.class, resultBlock.toBlock(), key_, acc_ ) );
            builder.add(
                    Expressions.return_(
                            null,
                            Expressions.call(
                                    childExp,
                                    BuiltInMethod.GROUP_BY2.method,
                                    Expressions.list(
                                            keySelector_,
                                            Expressions.call( lambdaFactory, BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_INITIALIZER.method ),
                                            Expressions.call( lambdaFactory, BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_ADDER.method ),
                                            Expressions.call( lambdaFactory, BuiltInMethod.AGG_LAMBDA_FACTORY_ACC_RESULT_SELECTOR.method, resultSelector_ ) ).appendIfNotNull( keyPhysType.comparer() ) ) ) );
        }
        return implementor.result( physType, builder.toBlock() );
    }


    private static boolean hasOrderedCall( List<AggImpState> aggs ) {
        for ( AggImpState agg : aggs ) {
            if ( !agg.call.collation.equals( AlgCollations.EMPTY ) ) {
                return true;
            }
        }
        return false;
    }


    private void declareParentAccumulator( List<Expression> initExpressions, BlockBuilder initBlock, PhysType accPhysType ) {
        if ( accPhysType.getJavaTupleType() instanceof SyntheticRecordType synType ) {
            // We have to initialize the SyntheticRecordType instance this way, to avoid using a class constructor with too many parameters.
            final ParameterExpression record0_ = Expressions.parameter( accPhysType.getJavaTupleType(), "record0" );
            initBlock.add( Expressions.declare( 0, record0_, null ) );
            initBlock.add( Expressions.statement( Expressions.assign( record0_, Expressions.new_( accPhysType.getJavaTupleType() ) ) ) );
            List<Types.RecordField> fieldList = synType.getRecordFields();
            for ( int i = 0; i < initExpressions.size(); i++ ) {
                Expression right = initExpressions.get( i );
                initBlock.add( Expressions.statement( Expressions.assign( Expressions.field( record0_, fieldList.get( i ) ), right ) ) );
            }
            initBlock.add( record0_ );
        } else {
            initBlock.add( accPhysType.record( initExpressions ) );
        }
    }


    /**
     * Implements the {@link AggregateLambdaFactory}.
     * <p>
     * Behavior depends upon ordering:
     * <ul>
     * <li>{@code hasOrderedCall == true} means there is at least one aggregate call including sort spec. We use {@link OrderedAggregateLambdaFactory} implementation to implement sorted aggregates for that.
     * <li>{@code hasOrderedCall == false} indicates to use {@link SequencedAdderAggregateLambdaFactory} to implement a non-sort aggregate.
     * </ul>
     */
    private void implementLambdaFactory( BlockBuilder builder, PhysType inputPhysType, List<AggImpState> aggs, Expression accumulatorInitializer, boolean hasOrderedCall, ParameterExpression lambdaFactory ) {
        if ( hasOrderedCall ) {
            ParameterExpression pe = Expressions.parameter( List.class, builder.newName( "sourceSorters" ) );
            builder.add( Expressions.declare( 0, pe, Expressions.new_( LinkedList.class ) ) );

            for ( AggImpState agg : aggs ) {
                if ( agg.call.collation.equals( AlgCollations.EMPTY ) ) {
                    continue;
                }
                final Pair<Expression, Expression> pair = inputPhysType.generateCollationKey( agg.call.collation.getFieldCollations() );
                builder.add( Expressions.statement( Expressions.call( pe, BuiltInMethod.COLLECTION_ADD.method, Expressions.new_( BuiltInMethod.SOURCE_SORTER.constructor, agg.accumulatorAdder, pair.left, pair.right ) ) ) );
            }
            builder.add( Expressions.declare( 0, lambdaFactory, Expressions.new_( BuiltInMethod.ORDERED_AGGREGATE_LAMBDA_FACTORY.constructor, accumulatorInitializer, pe ) ) );
        } else {
            // when hasOrderedCall == false
            ParameterExpression pe = Expressions.parameter( List.class, builder.newName( "accumulatorAdders" ) );
            builder.add( Expressions.declare( 0, pe, Expressions.new_( LinkedList.class ) ) );

            for ( AggImpState agg : aggs ) {
                builder.add( Expressions.statement( Expressions.call( pe, BuiltInMethod.COLLECTION_ADD.method, agg.accumulatorAdder ) ) );
            }
            builder.add( Expressions.declare( 0, lambdaFactory, Expressions.new_( BuiltInMethod.SEQUENCED_ADDER_AGGREGATE_LAMBDA_FACTORY.constructor, accumulatorInitializer, pe ) ) );
        }
    }


    /**
     * An implementation of {@link AggContext}.
     */
    private class AggContextImpl implements AggContext {

        private final AggImpState agg;
        private final JavaTypeFactory typeFactory;


        AggContextImpl( AggImpState agg, JavaTypeFactory typeFactory ) {
            this.agg = agg;
            this.typeFactory = typeFactory;
        }


        @Override
        public AggFunction aggregation() {
            return agg.call.getAggregation();
        }


        @Override
        public AlgDataType returnAlgType() {
            return agg.call.type;
        }


        @Override
        public Type returnType() {
            return EnumUtils.javaClass( typeFactory, returnAlgType() );
        }


        @Override
        public List<? extends AlgDataType> parameterAlgTypes() {
            return EnumUtils.fieldRowTypes( getInput().getTupleType(), null, agg.call.getArgList() );
        }


        @Override
        public List<? extends Type> parameterTypes() {
            return EnumUtils.fieldTypes( typeFactory, parameterAlgTypes() );
        }


        @Override
        public List<ImmutableBitSet> groupSets() {
            return groupSets;
        }


        @Override
        public List<Integer> keyOrdinals() {
            return groupSet.asList();
        }


        @Override
        public List<? extends AlgDataType> keyAlgTypes() {
            return EnumUtils.fieldRowTypes( getInput().getTupleType(), null, groupSet.asList() );
        }


        @Override
        public List<? extends Type> keyTypes() {
            return EnumUtils.fieldTypes( typeFactory, keyAlgTypes() );
        }

    }

}

