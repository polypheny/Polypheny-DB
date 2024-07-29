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

package org.polypheny.db.algebra.core;


import java.util.List;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptSamplingParameters;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Relational expression that returns a sample of the rows from its input.
 *
 * In SQL, a sample is expressed using the {@code TABLESAMPLE BERNOULLI} or {@code SYSTEM} keyword applied to a table, view or sub-query.
 */
public class Sample extends SingleAlg {

    private final AlgOptSamplingParameters params;


    public Sample( AlgCluster cluster, AlgNode child, AlgOptSamplingParameters params ) {
        super( cluster, cluster.traitSetOf( Convention.NONE ), child );
        this.params = params;
    }


    /**
     * Creates a Sample by parsing serialized output.
     */
    public Sample( AlgInput input ) {
        this( input.getCluster(), input.getInput(), getSamplingParameters( input ) );
    }


    private static AlgOptSamplingParameters getSamplingParameters( AlgInput input ) {
        String mode = input.getString( "mode" );
        float percentage = input.getFloat( "rate" );
        Object repeatableSeed = input.get( "repeatableSeed" );
        boolean repeatable = repeatableSeed instanceof Number;
        return new AlgOptSamplingParameters( mode.equals( "bernoulli" ), percentage, repeatable, repeatable ? ((Number) repeatableSeed).intValue() : 0 );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new Sample( getCluster(), sole( inputs ), params );
    }


    @Override
    public String algCompareString() {
        // Compare makes no sense here. Use hashCode() to avoid errors.
        return this.getClass().getSimpleName() + "$" + hashCode() + "&";
    }


    /**
     * Retrieve the sampling parameters for this Sample.
     */
    public AlgOptSamplingParameters getSamplingParameters() {
        return params;
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .item( "mode", params.isBernoulli() ? "bernoulli" : "system" )
                .item( "rate", params.getSamplingPercentage() )
                .item( "repeatableSeed", params.isRepeatable() ? params.getRepeatableSeed() : "-" );
    }

}
