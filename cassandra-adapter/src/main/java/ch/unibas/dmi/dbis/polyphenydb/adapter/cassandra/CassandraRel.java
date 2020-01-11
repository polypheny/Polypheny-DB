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

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra;


import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * Relational expression that uses Cassandra calling convention.
 */
public interface CassandraRel extends RelNode {

    void implement( Implementor implementor );

    /**
     * Calling convention for relational operations that occur in Cassandra.
     */
    Convention CONVENTION = new Convention.Impl( "CASSANDRA", CassandraRel.class );


    /**
     * Callback for the implementation process that converts a tree of {@link CassandraRel} nodes into a CQL query.
     */
    class Implementor {

        Type type = null;

        final List<Selector> selectFields = new ArrayList<>();
        final List<Relation> whereClause = new ArrayList<>();
        int offset = 0;
        int fetch = -1;
        final Map<String, ClusteringOrder> order = new LinkedHashMap<>();

        final List<Map<String, Term>> insertValues = new ArrayList<>();

        final List<Assignment> setAssignments = new ArrayList<>();

        RelOptTable table;
        CassandraTable cassandraTable;


        enum Type {
            SELECT,
            INSERT,
            UPDATE,
            DELETE
        }


        public void addWhereRelations(List<Relation> relations) {
            if ( relations != null ) {
                whereClause.addAll( relations );
            }
        }

        public void addInsertValues( List<Map<String, Term>> additionalValues ) {
            this.insertValues.addAll( additionalValues );
        }

        public void addSelectColumns( List<Selector> selectFields ) {
            this.selectFields.addAll( selectFields );
        }

        /*
         * Adds newly projected fields and restricted predicates.
         *
         * @param fields New fields to be projected from a query
         * @param predicates New predicates to be applied to the query
         */


        public void addAssignment(Assignment assignment) {

        }


        public void addOrder( Map<String, ClusteringOrder> newOrder ) {
            order.putAll( newOrder );
        }


        public void visitChild( int ordinal, RelNode input ) {
            assert ordinal == 0;
            ((CassandraRel) input).implement( this );
        }
    }
}

