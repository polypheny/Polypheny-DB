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


import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.enumerable.lpg.EnumerableLpgUnion;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Rule to convert an {@link LogicalRelUnion} to an {@link EnumerableUnion}.
 */
public class EnumerableUnionRule extends ConverterRule {

    EnumerableUnionRule() {
        this( LogicalRelUnion.class, "EnumerableUnionRule" );
    }


    EnumerableUnionRule( Class<? extends Union> type, String description ) {
        super( type, Convention.NONE, EnumerableConvention.INSTANCE, description );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        final Union union = (Union) alg;
        final EnumerableConvention out = EnumerableConvention.INSTANCE;
        final AlgTraitSet traitSet = union.getTraitSet().replace( out );
        List<AlgNode> inputs = convertList( union.getInputs(), out );
        if ( union instanceof LogicalLpgUnion ) {
            return new EnumerableLpgUnion( alg.getCluster(), traitSet, inputs, union.all );
        }
        return new EnumerableUnion( alg.getCluster(), traitSet, inputs, union.all );
    }

}

