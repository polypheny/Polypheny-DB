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


import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;


/**
 * Implementation of limits in Cassandra.
 */
public class CassandraLimit extends SingleAlg implements CassandraAlg {

    public final RexNode offset;
    public final RexNode fetch;


    public CassandraLimit( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input, RexNode offset, RexNode fetch ) {
        super( cluster, traitSet, input );
        this.offset = offset;
        this.fetch = fetch;
        assert getConvention() == input.getConvention();
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        // We do this so we get the limit for free
        return planner.getCostFactory().makeZeroCost();
    }


    @Override
    public CassandraLimit copy( AlgTraitSet traitSet, List<AlgNode> newInputs ) {
        return new CassandraLimit( getCluster(), traitSet, sole( newInputs ), offset, fetch );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                (getConvention() != null ? getConvention().getName() : "") + "$" +
                (offset != null ? offset.hashCode() + "$" : "") +
                (fetch != null ? fetch.hashCode() : "") + "&";
    }


    @Override
    public void implement( CassandraImplementContext context ) {
        context.visitChild( 0, getInput() );
        if ( offset != null ) {
            context.offset = RexLiteral.intValue( offset );
        }
        if ( fetch != null ) {
            context.fetch = RexLiteral.intValue( fetch );
        }
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        super.explainTerms( pw );
        pw.itemIf( "offset", offset, offset != null );
        pw.itemIf( "fetch", fetch, fetch != null );
        return pw;
    }

}

