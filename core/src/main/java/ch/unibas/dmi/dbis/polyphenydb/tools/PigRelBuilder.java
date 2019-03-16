/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.tools;


import ch.unibas.dmi.dbis.polyphenydb.plan.Context;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptSchema;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.JoinRelType;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.Ord;


/**
 * Extension to {@link RelBuilder} for Pig relational operators.
 */
public class PigRelBuilder extends RelBuilder {

    private String lastAlias;


    private PigRelBuilder( Context context, RelOptCluster cluster, RelOptSchema relOptSchema ) {
        super( context, cluster, relOptSchema );
    }


    /**
     * Creates a PigRelBuilder.
     */
    public static PigRelBuilder create( FrameworkConfig config ) {
        final RelBuilder relBuilder = RelBuilder.create( config );
        return new PigRelBuilder( config.getContext(), relBuilder.cluster, relBuilder.relOptSchema );
    }


    @Override
    public PigRelBuilder scan( String... tableNames ) {
        lastAlias = null;
        return (PigRelBuilder) super.scan( tableNames );
    }


    @Override
    public PigRelBuilder scan( Iterable<String> tableNames ) {
        lastAlias = null;
        return (PigRelBuilder) super.scan( tableNames );
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
    public PigRelBuilder load( String path, RexNode loadFunction, RelDataType rowType ) {
        scan( path.replace( ".csv", "" ) ); // TODO: use a UDT
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
    public PigRelBuilder distinct( Partitioner partitioner, int parallel ) {
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
    public PigRelBuilder group( GroupOption option, Partitioner partitioner, int parallel, GroupKey... groupKeys ) {
        return group( option, partitioner, parallel, ImmutableList.copyOf( groupKeys ) );
    }


    public PigRelBuilder group( GroupOption option, Partitioner partitioner, int parallel, Iterable<? extends GroupKey> groupKeys ) {
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
            RelNode r = null;
            if ( groupKey.i < n - 1 ) {
                r = build();
            }
            // Create a ROW to pass to COLLECT. Interestingly, this is not allowed by standard SQL; see [POLYPHENYDB-877] Allow ROW as argument to COLLECT.
            final RexNode row =
                    cluster.getRexBuilder().makeCall(
                            peek( 1, 0 ).getRowType(),
                            SqlStdOperatorTable.ROW,
                            fields() );
            aggregate( groupKey.e, aggregateCall( SqlStdOperatorTable.COLLECT, row ).as( getAlias() ) );
            if ( groupKey.i < n - 1 ) {
                push( r );
                List<RexNode> predicates = new ArrayList<>();
                for ( int key : Util.range( groupCount ) ) {
                    predicates.add( equals( field( 2, 0, key ), field( 2, 1, key ) ) );
                }
                join( JoinRelType.INNER, and( predicates ) );
            }
        }
        return this;
    }


    String getAlias() {
        if ( lastAlias != null ) {
            return lastAlias;
        } else {
            RelNode top = peek();
            if ( top instanceof TableScan ) {
                return Util.last( top.getTable().getQualifiedName() );
            } else {
                return null;
            }
        }
    }


    /**
     * As super-class method, but also retains alias for naming of aggregates.
     */
    @Override
    public RelBuilder as( final String alias ) {
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

