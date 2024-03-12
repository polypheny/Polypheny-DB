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

package org.polypheny.db.tools;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Util;


/**
 * Extension to {@link AlgBuilder} for Pig relational operators.
 */
public class PigAlgBuilder extends AlgBuilder {

    private String lastAlias;


    private PigAlgBuilder( Context context, AlgCluster cluster, Snapshot snapshot ) {
        super( context, cluster, snapshot );
    }


    public static PigAlgBuilder create( Statement statement, AlgCluster cluster ) {
        return new PigAlgBuilder( Contexts.EMPTY_CONTEXT, cluster, statement.getTransaction().getSnapshot() );
    }


    public static PigAlgBuilder create( Statement statement ) {
        final RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
        final AlgCluster cluster = AlgCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder, null, statement.getTransaction().getSnapshot() );
        return create( statement, cluster );
    }


    @Override
    public PigAlgBuilder relScan( String... tableNames ) {
        lastAlias = null;
        return (PigAlgBuilder) super.relScan( tableNames );
    }


    @Override
    public PigAlgBuilder relScan( List<String> tableNames ) {
        lastAlias = null;
        return (PigAlgBuilder) super.relScan( tableNames );
    }


    /**
     * Loads a data set.
     *
     * Equivalent to Pig Latin:
     * <pre>{@code LOAD 'path' USING loadFunction AS rowType}</pre>
     *
     * {@code loadFunction} and {@code rowType} are optional.
     *
     * @param path File path
     * @param loadFunction Load function
     * @param rowType Row type (what Pig calls 'schema')
     * @return This builder
     */
    public PigAlgBuilder load( String path, RexNode loadFunction, AlgDataType rowType ) {
        relScan( path.replace( ".csv", "" ) ); // TODO: use a UDT
        return this;
    }


    /**
     * Removes duplicate tuples in a relation.
     *
     * Equivalent Pig Latin:
     * <blockquote>
     * <pre>alias = DISTINCT alias [PARTITION BY partitioner] [PARALLEL n];</pre>
     * </blockquote>
     *
     * @param partitioner Partitioner; null means no partitioner
     * @param parallel Degree of parallelism; negative means unspecified
     * @return This builder
     */
    public PigAlgBuilder distinct( Partitioner partitioner, int parallel ) {
        // TODO: Use partitioner and parallel
        distinct();
        return this;
    }


    /**
     * Groups the data in one or more relations.
     *
     * Pig Latin syntax:
     * <blockquote>
     * alias = GROUP alias { ALL | BY expression }
     * [, alias ALL | BY expression ...]
     * [USING 'collected' | 'merge'] [PARTITION BY partitioner] [PARALLEL n];
     * </blockquote>
     *
     * @param groupKeys One of more group keys; use {@link #groupKey()} for ALL
     * @param option Whether to use an optimized method combining the data (COLLECTED for one input or MERGE for two or more inputs)
     * @param partitioner Partitioner; null means no partitioner
     * @param parallel Degree of parallelism; negative means unspecified
     * @return This builder
     */
    public PigAlgBuilder group( GroupOption option, Partitioner partitioner, int parallel, GroupKey... groupKeys ) {
        return group( option, partitioner, parallel, ImmutableList.copyOf( groupKeys ) );
    }


    public PigAlgBuilder group( GroupOption option, Partitioner partitioner, int parallel, Iterable<? extends GroupKey> groupKeys ) {
        @SuppressWarnings("unchecked") final List<GroupKeyImpl> groupKeyList = ImmutableList.copyOf( (Iterable) groupKeys );
        if ( groupKeyList.isEmpty() ) {
            throw new IllegalArgumentException( "must have at least one group" );
        }
        final int groupCount = groupKeyList.get( 0 ).nodes.size();
        for ( GroupKeyImpl groupKey : groupKeyList ) {
            if ( groupKey.nodes.size() != groupCount ) {
                throw new IllegalArgumentException( "group key size mismatch" );
            }
        }
        final int n = groupKeyList.size();
        for ( Ord<GroupKeyImpl> groupKey : Ord.reverse( groupKeyList ) ) {
            AlgNode r = null;
            if ( groupKey.i < n - 1 ) {
                r = build();
            }
            // Create a ROW to pass to COLLECT. Interestingly, this is not allowed by standard SQL; see [POLYPHENYDB-877] Allow ROW as argument to COLLECT.
            final RexNode row =
                    cluster.getRexBuilder().makeCall(
                            peek( 1, 0 ).getTupleType(),
                            OperatorRegistry.get( OperatorName.ROW ),
                            fields() );
            aggregate( groupKey.e, aggregateCall( OperatorRegistry.getAgg( OperatorName.COLLECT ), row ).as( getAlias() ) );
            if ( groupKey.i < n - 1 ) {
                push( r );
                List<RexNode> predicates = new ArrayList<>();
                for ( int key : Util.range( groupCount ) ) {
                    predicates.add( equals( field( 2, 0, key ), field( 2, 1, key ) ) );
                }
                join( JoinAlgType.INNER, and( predicates ) );
            }
        }
        return this;
    }


    String getAlias() {
        if ( lastAlias != null ) {
            return lastAlias;
        } else {
            AlgNode top = peek();
            if ( top instanceof RelScan ) {
                return top.getEntity().name;
            } else {
                return null;
            }
        }
    }


    /**
     * As super-class method, but also retains alias for naming of aggregates.
     */
    @Override
    public AlgBuilder as( final String alias ) {
        lastAlias = alias;
        return super.as( alias );
    }


    /**
     * Partitioner for group and join
     */
    interface Partitioner {

    }


    /**
     * Option for performing group efficiently if data set is already sorted
     */
    public enum GroupOption {
        MERGE,
        COLLECTED
    }

}

