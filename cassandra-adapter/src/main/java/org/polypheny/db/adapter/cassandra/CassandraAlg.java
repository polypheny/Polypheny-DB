/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.adapter.cassandra;


import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AlgOptTable;


/**
 * Relational expression that uses Cassandra calling convention.
 */
public interface CassandraAlg extends AlgNode {

    void implement( CassandraImplementContext context );


    /**
     * Context to convert a tree of {@link CassandraAlg} nodes into a CQL query.
     */
    class CassandraImplementContext {
        // TODO JS: Find a better name for this class

        Type type = null;

        final List<Selector> selectFields = new ArrayList<>();
        final List<Relation> whereClause = new ArrayList<>();
        int offset = 0;
        int fetch = -1;
        final Map<String, ClusteringOrder> order = new LinkedHashMap<>();

        final List<Map<String, Term>> insertValues = new ArrayList<>();
        boolean ifNotExists = false;

        final List<Assignment> setAssignments = new ArrayList<>();

        AlgOptTable table;
        CassandraTable cassandraTable;

        AlgCollation filterCollation = null;


        public enum Type {
            SELECT,
            INSERT,
            UPDATE,
            DELETE
        }


        public void addWhereRelations( List<Relation> relations ) {
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


        public void addAssignments( List<Assignment> assignments ) {
            this.setAssignments.addAll( assignments );
        }


        public void addOrder( Map<String, ClusteringOrder> newOrder ) {
            order.putAll( newOrder );
        }


        public void visitChild( int ordinal, AlgNode input ) {
            assert ordinal == 0;
            ((CassandraAlg) input).implement( this );
        }

    }

}

