/*
 * Copyright 2019-2020 The Polypheny Project
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


import com.datastax.oss.driver.api.querybuilder.select.Selector;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.adapter.cassandra.rules.CassandraRules;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;


/**
 * Implementation of {@link Project} relational expression in Cassandra.
 */
public class CassandraProject extends Project implements CassandraRel {

    public CassandraProject( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, List<? extends RexNode> projects, RelDataType rowType ) {
        super( cluster, traitSet, input, projects, rowType );
        // TODO JS: Check this
//        assert getConvention() == CassandraRel.CONVENTION;
//        assert getConvention() == input.getConvention();
    }


    @Override
    public Project copy( RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType ) {
        return new CassandraProject( getCluster(), traitSet, input, projects, rowType );
    }


    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.8 );
    }


    @Override
    public void implement( CassandraImplementContext context ) {
        context.visitChild( 0, getInput() );
        final CassandraRules.RexToCassandraTranslator translator = new CassandraRules.RexToCassandraTranslator( (JavaTypeFactory) getCluster().getTypeFactory(), CassandraRules.cassandraFieldNames( getInput().getRowType() ) );
        final List<Selector> fields = new ArrayList<>();
        for ( Pair<RexNode, String> pair : getNamedProjects() ) {
            if ( pair.left instanceof RexInputRef ) {
                final String name = pair.right;
                final String originalName = pair.left.accept( translator );
                fields.add( Selector.column( originalName ).as( name ) );
            }
        }
        context.addSelectColumns( fields );
    }
}

