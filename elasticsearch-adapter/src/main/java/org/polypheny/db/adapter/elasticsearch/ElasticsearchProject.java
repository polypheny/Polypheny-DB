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

package org.polypheny.db.adapter.elasticsearch;


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;


/**
 * Implementation of {@link org.polypheny.db.algebra.core.Project} relational expression in Elasticsearch.
 */
public class ElasticsearchProject extends Project implements ElasticsearchRel {

    ElasticsearchProject( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input, List<? extends RexNode> projects, AlgDataType rowType ) {
        super( cluster, traitSet, input, projects, rowType );
        assert getConvention() == ElasticsearchRel.CONVENTION;
        assert getConvention() == input.getConvention();
    }


    @Override
    public Project copy( AlgTraitSet algTraitSet, AlgNode input, List<RexNode> projects, AlgDataType algDataType ) {
        return new ElasticsearchProject( getCluster(), traitSet, input, projects, algDataType );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );

        final List<String> inFields = ElasticsearchRules.elasticsearchFieldNames( getInput().getRowType() );
        final ElasticsearchRules.RexToElasticsearchTranslator translator = new ElasticsearchRules.RexToElasticsearchTranslator( (JavaTypeFactory) getCluster().getTypeFactory(), inFields );

        final List<String> fields = new ArrayList<>();
        final List<String> scriptFields = new ArrayList<>();
        for ( Pair<RexNode, String> pair : getNamedProjects() ) {
            final String name = pair.right;
            final String expr = pair.left.accept( translator );

            if ( ElasticsearchRules.isItem( pair.left ) ) {
                implementor.addExpressionItemMapping( name, ElasticsearchRules.stripQuotes( expr ) );
            }

            if ( expr.equals( name ) ) {
                fields.add( name );
            } else if ( expr.matches( "\"literal\":.+" ) ) {
                scriptFields.add( ElasticsearchRules.quote( name ) + ":{\"script\": " + expr.split( ":" )[1] + "}" );
            } else {
                scriptFields.add( ElasticsearchRules.quote( name ) + ":{\"script\":"
                        // _source (ES2) vs params._source (ES5)
                        + "\"" + implementor.elasticsearchTable.scriptedFieldPrefix() + "." + expr.replaceAll( "\"", "" ) + "\"}" );
            }
        }

        StringBuilder query = new StringBuilder();
        if ( scriptFields.isEmpty() ) {
            List<String> newList = fields.stream().map( ElasticsearchRules::quote ).collect( Collectors.toList() );

            final String findString = String.join( ", ", newList );
            query.append( "\"_source\" : [" ).append( findString ).append( "]" );
        } else {
            // if scripted fields are present, ES ignores _source attribute
            for ( String field : fields ) {
                scriptFields.add( ElasticsearchRules.quote( field ) + ":{\"script\": "
                        // _source (ES2) vs params._source (ES5)
                        + "\"" + implementor.elasticsearchTable.scriptedFieldPrefix() + "." + field + "\"}" );
            }
            query.append( "\"script_fields\": {" + String.join( ", ", scriptFields ) + "}" );
        }

        implementor.list.removeIf( l -> l.startsWith( "\"_source\"" ) );
        implementor.add( "{" + query.toString() + "}" );
    }

}

