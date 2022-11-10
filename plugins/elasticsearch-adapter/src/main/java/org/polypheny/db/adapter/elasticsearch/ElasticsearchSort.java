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


import java.util.List;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;


/**
 * Implementation of {@link org.polypheny.db.algebra.core.Sort} relational expression in Elasticsearch.
 */
public class ElasticsearchSort extends Sort implements ElasticsearchRel {

    ElasticsearchSort( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode child, AlgCollation collation, RexNode offset, RexNode fetch ) {
        super( cluster, traitSet, child, collation, offset, fetch );
        assert getConvention() == CONVENTION;
        assert getConvention() == child.getConvention();
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.05 );
    }


    @Override
    public Sort copy( AlgTraitSet traitSet, AlgNode algNode, AlgCollation algCollation, RexNode offset, RexNode fetch ) {
        return new ElasticsearchSort( getCluster(), traitSet, algNode, collation, offset, fetch );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );
        final List<AlgDataTypeField> fields = getRowType().getFieldList();

        for ( AlgFieldCollation fieldCollation : collation.getFieldCollations() ) {
            final String name = fields.get( fieldCollation.getFieldIndex() ).getName();
            final String rawName = implementor.expressionItemMap.getOrDefault( name, name );
            implementor.addSort( rawName, fieldCollation.getDirection() );
        }

        if ( offset != null ) {
            implementor.offset( ((RexLiteral) offset).getValueAs( Long.class ) );
        }

        if ( fetch != null ) {
            implementor.fetch( ((RexLiteral) fetch).getValueAs( Long.class ) );
        }
    }

}
