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
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.metadata.AlgMdUtil;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;


/**
 * Relational expression that returns the rows of its first input minus any matching rows from its other inputs.
 *
 * Corresponds to the SQL {@code EXCEPT} operator.
 *
 * If "all" is true, then multiset subtraction is performed; otherwise, set subtraction is performed (implying no duplicates in the results).
 */
public abstract class Minus extends SetOp {

    public Minus( AlgCluster cluster, AlgTraitSet traits, List<AlgNode> inputs, boolean all ) {
        super( cluster, traits, inputs, Kind.EXCEPT, all );
    }


    /**
     * Creates a Minus by parsing serialized output.
     */
    protected Minus( AlgInput input ) {
        super( input );
    }


    @Override
    public double estimateTupleCount( AlgMetadataQuery mq ) {
        return AlgMdUtil.getMinusRowCount( mq, this );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                inputs.stream().map( AlgNode::algCompareString ).collect( Collectors.joining( "$" ) ) + "$" +
                all + "&";
    }

}
