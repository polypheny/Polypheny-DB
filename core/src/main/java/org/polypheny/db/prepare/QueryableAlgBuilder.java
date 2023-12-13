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

package org.polypheny.db.prepare;


import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import lombok.Setter;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Grouping;
import org.apache.calcite.linq4j.OrderedQueryable;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.QueryableDefaults;
import org.apache.calcite.linq4j.QueryableFactory;
import org.apache.calcite.linq4j.function.BigDecimalFunction1;
import org.apache.calcite.linq4j.function.DoubleFunction1;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.FloatFunction1;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.IntegerFunction1;
import org.apache.calcite.linq4j.function.LongFunction1;
import org.apache.calcite.linq4j.function.NullableBigDecimalFunction1;
import org.apache.calcite.linq4j.function.NullableDoubleFunction1;
import org.apache.calcite.linq4j.function.NullableFloatFunction1;
import org.apache.calcite.linq4j.function.NullableIntegerFunction1;
import org.apache.calcite.linq4j.function.NullableLongFunction1;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.function.Predicate2;
import org.apache.calcite.linq4j.tree.FunctionExpression;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.impl.AbstractEntityQueryable;
import org.polypheny.db.schema.types.QueryableEntity;
import org.polypheny.db.schema.types.TranslatableEntity;


/**
 * Implementation of {@link QueryableFactory} that builds a tree of {@link AlgNode} planner nodes. Used by {@link LixToAlgTranslator}.
 * <p>
 * Each of the methods that implements a {@code Replayer} method creates a tree of {@code AlgNode}s equivalent to the
 * arguments, and calls {@link #setAlg} to assign the root of that tree to the {@link #alg} member variable.
 * <p>
 * To comply with the {@link org.apache.calcite.linq4j.QueryableFactory} interface, which is after all a factory, each method
 * returns a dummy result such as {@code null} or {@code 0}. The caller will not use the result. The real effect of the method
 * is to call {@link #setAlg} with a {@code AlgNode}.
 * <p>
 * NOTE: Many methods currently throw {@link UnsupportedOperationException}. These method need to be implemented.
 *
 * @param <T> Element type
 */
class QueryableAlgBuilder<T> implements QueryableFactory<T> {

    private final LixToAlgTranslator translator;
    @Setter
    private AlgNode alg;


    QueryableAlgBuilder( LixToAlgTranslator translator ) {
        this.translator = translator;
    }


    AlgNode toAlg( Queryable<T> queryable ) {
        if ( queryable instanceof QueryableDefaults.Replayable ) {
            //noinspection unchecked
            ((QueryableDefaults.Replayable) queryable).replay( this );
            return alg;
        }
        if ( queryable instanceof AbstractEntityQueryable ) {
            final AbstractEntityQueryable tableQueryable = (AbstractEntityQueryable) queryable;
            final QueryableEntity table = tableQueryable.entity.unwrap( QueryableEntity.class ).orElseThrow();

            if ( table instanceof TranslatableEntity ) {
                return ((TranslatableEntity) table).toAlg( translator.cluster, translator.cluster.traitSet() );
            } else {
                return LogicalRelScan.create( translator.cluster, null );
            }
        }
        return translator.translate( queryable.getExpression() );
    }


    @Override
    public <TAccumulate, TResult> TResult aggregate(
            Queryable<T> source,
            TAccumulate seed,
            FunctionExpression<Function2<TAccumulate, T, TAccumulate>> func,
            FunctionExpression<Function1<TAccumulate, TResult>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T aggregate( Queryable<T> source, FunctionExpression<Function2<T, T, T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TAccumulate> TAccumulate aggregate(
            Queryable<T> source,
            TAccumulate seed,
            FunctionExpression<Function2<TAccumulate, T, TAccumulate>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean all( Queryable<T> source, FunctionExpression<Predicate1<T>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean any( Queryable<T> source ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean any( Queryable<T> source, FunctionExpression<Predicate1<T>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public BigDecimal averageBigDecimal( Queryable<T> source, FunctionExpression<BigDecimalFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public BigDecimal averageNullableBigDecimal( Queryable<T> source, FunctionExpression<NullableBigDecimalFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public double averageDouble( Queryable<T> source, FunctionExpression<DoubleFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Double averageNullableDouble( Queryable<T> source, FunctionExpression<NullableDoubleFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public int averageInteger( Queryable<T> source, FunctionExpression<IntegerFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Integer averageNullableInteger( Queryable<T> source, FunctionExpression<NullableIntegerFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public float averageFloat( Queryable<T> source, FunctionExpression<FloatFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Float averageNullableFloat( Queryable<T> source, FunctionExpression<NullableFloatFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public long averageLong( Queryable<T> source, FunctionExpression<LongFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Long averageNullableLong( Queryable<T> source, FunctionExpression<NullableLongFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> concat( Queryable<T> source, Enumerable<T> source2 ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean contains( Queryable<T> source, T element ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean contains( Queryable<T> source, T element, EqualityComparer<T> comparer ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public int count( Queryable<T> source ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public int count( Queryable<T> source, FunctionExpression<Predicate1<T>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> defaultIfEmpty( Queryable<T> source ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> defaultIfEmpty( Queryable<T> source, T value ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> distinct( Queryable<T> source ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> distinct( Queryable<T> source, EqualityComparer<T> comparer ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T elementAt( Queryable<T> source, int index ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T elementAtOrDefault( Queryable<T> source, int index ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> except( Queryable<T> source, Enumerable<T> enumerable ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> except( Queryable<T> source, Enumerable<T> enumerable, boolean all ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> except( Queryable<T> source, Enumerable<T> enumerable, EqualityComparer<T> tEqualityComparer ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> except( Queryable<T> source, Enumerable<T> enumerable, EqualityComparer<T> comparer, boolean all ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T first( Queryable<T> source ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T first( Queryable<T> source, FunctionExpression<Predicate1<T>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T firstOrDefault( Queryable<T> source ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T firstOrDefault( Queryable<T> source, FunctionExpression<Predicate1<T>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey> Queryable<Grouping<TKey, T>> groupBy( Queryable<T> source, FunctionExpression<Function1<T, TKey>> keySelector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey> Queryable<Grouping<TKey, T>> groupBy(
            Queryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector,
            EqualityComparer<TKey> comparer ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
            Queryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector,
            FunctionExpression<Function1<T, TElement>> elementSelector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey, TResult> Queryable<TResult> groupByK(
            Queryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector,
            FunctionExpression<Function2<TKey, Enumerable<T>, TResult>> resultSelector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey, TElement> Queryable<Grouping<TKey, TElement>> groupBy(
            Queryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector,
            FunctionExpression<Function1<T, TElement>> elementSelector,
            EqualityComparer<TKey> comparer ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey, TResult> Queryable<TResult> groupByK(
            Queryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector,
            FunctionExpression<Function2<TKey, Enumerable<T>, TResult>> elementSelector,
            EqualityComparer<TKey> comparer ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey, TElement, TResult> Queryable<TResult> groupBy(
            Queryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector,
            FunctionExpression<Function1<T, TElement>> elementSelector,
            FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>> resultSelector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey, TElement, TResult> Queryable<TResult> groupBy(
            Queryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector,
            FunctionExpression<Function1<T, TElement>> elementSelector,
            FunctionExpression<Function2<TKey, Enumerable<TElement>, TResult>> resultSelector,
            EqualityComparer<TKey> comparer ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
            Queryable<T> source,
            Enumerable<TInner> inner,
            FunctionExpression<Function1<T, TKey>> outerKeySelector,
            FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
            FunctionExpression<Function2<T, Enumerable<TInner>, TResult>> resultSelector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TInner, TKey, TResult> Queryable<TResult> groupJoin(
            Queryable<T> source,
            Enumerable<TInner> inner,
            FunctionExpression<Function1<T, TKey>> outerKeySelector,
            FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
            FunctionExpression<Function2<T, Enumerable<TInner>, TResult>> resultSelector,
            EqualityComparer<TKey> comparer ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> intersect( Queryable<T> source, Enumerable<T> enumerable ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> intersect( Queryable<T> source, Enumerable<T> enumerable, boolean all ) {
        return null;
    }


    @Override
    public Queryable<T> intersect( Queryable<T> source, Enumerable<T> enumerable, EqualityComparer<T> tEqualityComparer ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> intersect( Queryable<T> source, Enumerable<T> enumerable, EqualityComparer<T> comparer, boolean all ) {
        return null;
    }


    @Override
    public <TInner, TKey, TResult> Queryable<TResult> join(
            Queryable<T> source,
            Enumerable<TInner> inner,
            FunctionExpression<Function1<T, TKey>> outerKeySelector,
            FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
            FunctionExpression<Function2<T, TInner, TResult>> resultSelector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TInner, TKey, TResult> Queryable<TResult> join(
            Queryable<T> source,
            Enumerable<TInner> inner,
            FunctionExpression<Function1<T, TKey>> outerKeySelector,
            FunctionExpression<Function1<TInner, TKey>> innerKeySelector,
            FunctionExpression<Function2<T, TInner, TResult>> resultSelector,
            EqualityComparer<TKey> comparer ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T last( Queryable<T> source ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T last( Queryable<T> source, FunctionExpression<Predicate1<T>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T lastOrDefault( Queryable<T> source ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T lastOrDefault( Queryable<T> source, FunctionExpression<Predicate1<T>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public long longCount( Queryable<T> source ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public long longCount( Queryable<T> source, FunctionExpression<Predicate1<T>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T max( Queryable<T> source ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TResult extends Comparable<TResult>> TResult max( Queryable<T> source, FunctionExpression<Function1<T, TResult>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T min( Queryable<T> source ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TResult extends Comparable<TResult>> TResult min( Queryable<T> source, FunctionExpression<Function1<T, TResult>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TResult> Queryable<TResult> ofType( Queryable<T> source, Class<TResult> clazz ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <T2> Queryable<T2> cast( Queryable<T> source, Class<T2> clazz ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey extends Comparable> OrderedQueryable<T> orderBy(
            Queryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey> OrderedQueryable<T> orderBy(
            Queryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector,
            Comparator<TKey> comparator ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey extends Comparable> OrderedQueryable<T> orderByDescending(
            Queryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey> OrderedQueryable<T> orderByDescending(
            Queryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector,
            Comparator<TKey> comparator ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> reverse( Queryable<T> source ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TResult> Queryable<TResult> select( Queryable<T> source, FunctionExpression<Function1<T, TResult>> selector ) {
        AlgNode child = toAlg( source );
        List<RexNode> nodes = translator.toRexList( selector, child );
        setAlg( LogicalProject.create( child, nodes, (List<String>) null ) );
        return null;
    }


    @Override
    public <TResult> Queryable<TResult> selectN(
            Queryable<T> source,
            FunctionExpression<Function2<T, Integer, TResult>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TResult> Queryable<TResult> selectMany(
            Queryable<T> source,
            FunctionExpression<Function1<T, Enumerable<TResult>>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TResult> Queryable<TResult> selectManyN(
            Queryable<T> source,
            FunctionExpression<Function2<T, Integer, Enumerable<TResult>>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TCollection, TResult> Queryable<TResult> selectMany(
            Queryable<T> source,
            FunctionExpression<Function2<T, Integer, Enumerable<TCollection>>> collectionSelector,
            FunctionExpression<Function2<T, TCollection, TResult>> resultSelector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TCollection, TResult> Queryable<TResult> selectManyN(
            Queryable<T> source,
            FunctionExpression<Function1<T, Enumerable<TCollection>>> collectionSelector,
            FunctionExpression<Function2<T, TCollection, TResult>> resultSelector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean sequenceEqual( Queryable<T> source, Enumerable<T> enumerable ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public boolean sequenceEqual( Queryable<T> source, Enumerable<T> enumerable, EqualityComparer<T> tEqualityComparer ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T single( Queryable<T> source ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T single( Queryable<T> source, FunctionExpression<Predicate1<T>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T singleOrDefault( Queryable<T> source ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public T singleOrDefault( Queryable<T> source, FunctionExpression<Predicate1<T>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> skip( Queryable<T> source, int count ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> skipWhile( Queryable<T> source, FunctionExpression<Predicate1<T>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> skipWhileN( Queryable<T> source, FunctionExpression<Predicate2<T, Integer>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public BigDecimal sumBigDecimal( Queryable<T> source, FunctionExpression<BigDecimalFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public BigDecimal sumNullableBigDecimal( Queryable<T> source, FunctionExpression<NullableBigDecimalFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public double sumDouble( Queryable<T> source, FunctionExpression<DoubleFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Double sumNullableDouble( Queryable<T> source, FunctionExpression<NullableDoubleFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public int sumInteger( Queryable<T> source, FunctionExpression<IntegerFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Integer sumNullableInteger( Queryable<T> source, FunctionExpression<NullableIntegerFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public long sumLong( Queryable<T> source, FunctionExpression<LongFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Long sumNullableLong( Queryable<T> source, FunctionExpression<NullableLongFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public float sumFloat( Queryable<T> source, FunctionExpression<FloatFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Float sumNullableFloat( Queryable<T> source, FunctionExpression<NullableFloatFunction1<T>> selector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> take( Queryable<T> source, int count ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> takeWhile( Queryable<T> source, FunctionExpression<Predicate1<T>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> takeWhileN( Queryable<T> source, FunctionExpression<Predicate2<T, Integer>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey extends Comparable<TKey>> OrderedQueryable<T> thenBy(
            OrderedQueryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey> OrderedQueryable<T> thenBy(
            OrderedQueryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector,
            Comparator<TKey> comparator ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey extends Comparable<TKey>> OrderedQueryable<T> thenByDescending(
            OrderedQueryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <TKey> OrderedQueryable<T> thenByDescending(
            OrderedQueryable<T> source,
            FunctionExpression<Function1<T, TKey>> keySelector,
            Comparator<TKey> comparator ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> union( Queryable<T> source, Enumerable<T> source1 ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> union( Queryable<T> source, Enumerable<T> source1, EqualityComparer<T> tEqualityComparer ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public Queryable<T> where( Queryable<T> source, FunctionExpression<? extends Predicate1<T>> predicate ) {
        AlgNode child = toAlg( source );
        RexNode node = translator.toRex( predicate, child );
        setAlg( LogicalFilter.create( child, node ) );
        return source;
    }


    @Override
    public Queryable<T> whereN( Queryable<T> source, FunctionExpression<? extends Predicate2<T, Integer>> predicate ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public <T1, TResult> Queryable<TResult> zip(
            Queryable<T> source,
            Enumerable<T1> source1,
            FunctionExpression<Function2<T, T1, TResult>> resultSelector ) {
        throw new UnsupportedOperationException();
    }

}
