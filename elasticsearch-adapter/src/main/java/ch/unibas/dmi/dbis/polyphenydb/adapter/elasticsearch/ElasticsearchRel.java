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

package ch.unibas.dmi.dbis.polyphenydb.adapter.elasticsearch;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * Relational expression that uses Elasticsearch calling convention.
 */
public interface ElasticsearchRel extends RelNode {

    void implement( Implementor implementor );

    /**
     * Calling convention for relational operations that occur in Elasticsearch.
     */
    Convention CONVENTION = new Convention.Impl( "ELASTICSEARCH", ElasticsearchRel.class );


    /**
     * Callback for the implementation process that converts a tree of {@link ElasticsearchRel} nodes into an Elasticsearch query.
     */
    class Implementor {

        final List<String> list = new ArrayList<>();

        /**
         * Sorting clauses.
         *
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html">Sort</a>
         */
        final List<Map.Entry<String, RelFieldCollation.Direction>> sort = new ArrayList<>();

        /**
         * Elastic aggregation ({@code MIN / MAX / COUNT} etc.) statements (functions).
         *
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html">aggregations</a>
         */
        final List<Map.Entry<String, String>> aggregations = new ArrayList<>();

        /**
         * Allows bucketing documents together. Similar to {@code select ... from table group by field1}
         *
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/6.3/search-aggregations-bucket.html">Bucket Aggregrations</a>
         */
        final List<String> groupBy = new ArrayList<>();

        /**
         * Keeps mapping between Polypheny-DB expression identifier (like {@code EXPR$0}) and original item call like {@code _MAP['foo.bar']} ({@code foo.bar} really). This information otherwise might be lost during query translation.
         *
         * @see SqlStdOperatorTable#ITEM
         */
        final Map<String, String> expressionItemMap = new LinkedHashMap<>();

        /**
         * Starting index (default {@code 0}). Equivalent to {@code start} in ES query.
         *
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html">From/Size</a>
         */
        Long offset;

        /**
         * Number of records to return. Equivalent to {@code size} in ES query.
         *
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-from-size.html">From/Size</a>
         */
        Long fetch;

        RelOptTable table;
        ElasticsearchTable elasticsearchTable;


        void add( String findOp ) {
            list.add( findOp );
        }


        void addGroupBy( String field ) {
            Objects.requireNonNull( field, "field" );
            groupBy.add( field );
        }


        void addSort( String field, RelFieldCollation.Direction direction ) {
            Objects.requireNonNull( field, "field" );
            sort.add( new Pair<>( field, direction ) );
        }


        void addAggregation( String field, String expression ) {
            Objects.requireNonNull( field, "field" );
            Objects.requireNonNull( expression, "expression" );
            aggregations.add( new Pair<>( field, expression ) );
        }


        void addExpressionItemMapping( String expressionId, String item ) {
            Objects.requireNonNull( expressionId, "expressionId" );
            Objects.requireNonNull( item, "item" );
            expressionItemMap.put( expressionId, item );
        }


        void offset( long offset ) {
            this.offset = offset;
        }


        void fetch( long fetch ) {
            this.fetch = fetch;
        }


        void visitChild( int ordinal, RelNode input ) {
            assert ordinal == 0;
            ((ElasticsearchRel) input).implement( this );
        }

    }
}
