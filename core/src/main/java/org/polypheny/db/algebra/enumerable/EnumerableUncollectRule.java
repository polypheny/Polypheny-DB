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
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.Uncollect;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Rule to convert an {@link org.polypheny.db.algebra.core.Uncollect} to an {@link EnumerableUncollect}.
 */
public class EnumerableUncollectRule extends ConverterRule {

    EnumerableUncollectRule() {
        super( Uncollect.class, Convention.NONE, EnumerableConvention.INSTANCE, "EnumerableUncollectRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final Uncollect uncollect = (Uncollect) alg;
        final AlgTraitSet traitSet = uncollect.getTraitSet().replace( EnumerableConvention.INSTANCE );
        final AlgNode input = uncollect.getInput();
        final AlgNode newInput = convert( input, input.getTraitSet().replace( EnumerableConvention.INSTANCE ) );
        return EnumerableUncollect.create( traitSet, newInput, uncollect.withOrdinality );
    }

}

