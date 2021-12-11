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


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.Objects;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;


/**
 * Implementation of a {@link org.polypheny.db.algebra.core.Filter} relational expression in Elasticsearch.
 */
public class ElasticsearchFilter extends Filter implements ElasticsearchRel {

    ElasticsearchFilter( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode child, RexNode condition ) {
        super( cluster, traitSet, child, condition );
        assert getConvention() == CONVENTION;
        assert getConvention() == child.getConvention();
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public Filter copy( AlgTraitSet algTraitSet, AlgNode input, RexNode condition ) {
        return new ElasticsearchFilter( getCluster(), algTraitSet, input, condition );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );
        ObjectMapper mapper = implementor.elasticsearchTable.mapper;
        PredicateAnalyzerTranslator translator = new PredicateAnalyzerTranslator( mapper );
        try {
            implementor.add( translator.translateMatch( condition ) );
        } catch ( IOException e ) {
            throw new UncheckedIOException( e );
        } catch ( PredicateAnalyzer.ExpressionNotAnalyzableException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * New version of translator which uses visitor pattern and allow to process more complex (boolean) predicates.
     */
    static class PredicateAnalyzerTranslator {

        private final ObjectMapper mapper;


        PredicateAnalyzerTranslator( final ObjectMapper mapper ) {
            this.mapper = Objects.requireNonNull( mapper, "mapper" );
        }


        String translateMatch( RexNode condition ) throws IOException, PredicateAnalyzer.ExpressionNotAnalyzableException {
            StringWriter writer = new StringWriter();
            JsonGenerator generator = mapper.getFactory().createGenerator( writer );
            QueryBuilders.constantScoreQuery( PredicateAnalyzer.analyze( condition ) ).writeJson( generator );
            generator.flush();
            generator.close();
            return "{\"query\" : " + writer.toString() + "}";
        }

    }

}
