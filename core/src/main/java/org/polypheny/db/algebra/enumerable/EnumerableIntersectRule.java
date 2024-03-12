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
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Rule to convert a {@link LogicalRelIntersect} to an {@link EnumerableIntersect}.
 */
public class EnumerableIntersectRule extends ConverterRule {

    EnumerableIntersectRule() {
        super( LogicalRelIntersect.class, Convention.NONE, EnumerableConvention.INSTANCE, "EnumerableIntersectRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final LogicalRelIntersect intersect = (LogicalRelIntersect) alg;
        if ( intersect.all ) {
            return null; // INTERSECT ALL not implemented
        }
        final EnumerableConvention out = EnumerableConvention.INSTANCE;
        final AlgTraitSet traitSet = intersect.getTraitSet().replace( out );
        return new EnumerableIntersect( alg.getCluster(), traitSet, convertList( intersect.getInputs(), out ), false );
    }

}

