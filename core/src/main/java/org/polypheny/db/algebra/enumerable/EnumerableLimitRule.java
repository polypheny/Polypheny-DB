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

package org.polypheny.db.algebra.enumerable;


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Rule to convert an {@link Sort} that has {@code offset} or {@code fetch} set to an {@link EnumerableLimit} on top of a "pure" {@code Sort} that has no offset or fetch.
 */
public class EnumerableLimitRule extends AlgOptRule {

    EnumerableLimitRule() {
        super( operand( Sort.class, any() ), "EnumerableLimitRule" );
    }


    @Override
    public void onMatch( AlgOptRuleCall call ) {
        final Sort sort = call.alg( 0 );
        if ( sort.offset == null && sort.fetch == null ) {
            return;
        }
        final AlgTraitSet traitSet = sort.getTraitSet().replace( EnumerableConvention.INSTANCE );
        AlgNode input = sort.getInput();
        if ( !sort.getCollation().getFieldCollations().isEmpty() ) {
            // Create a sort with the same sort key, but no offset or fetch.
            input = sort.copy( sort.getTraitSet(), input, sort.getCollation(), null, null, null );
        }
        AlgNode x = convert( input, input.getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        call.transformTo( new EnumerableLimit( sort.getCluster(), traitSet, x, sort.offset, sort.fetch ) );
    }

}

