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

package org.polypheny.db.algebra.stream;


import java.util.List;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Sub-class of {@link Delta} not targeted at any particular engine or calling convention.
 */
public final class LogicalDelta extends Delta {

    /**
     * Creates a LogicalDelta.
     *
     * Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param input Input relational expression
     */
    public LogicalDelta( AlgCluster cluster, AlgTraitSet traits, AlgNode input ) {
        super( cluster, traits, input );
    }


    /**
     * Creates a LogicalDelta by parsing serialized output.
     */
    public LogicalDelta( AlgInput input ) {
        super( input );
    }


    /**
     * Creates a LogicalDelta.
     */
    public static LogicalDelta create( AlgNode input ) {
        final AlgTraitSet traitSet = input.getTraitSet().replace( Convention.NONE );
        return new LogicalDelta( input.getCluster(), traitSet, input );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalDelta( getCluster(), traitSet, AbstractAlgNode.sole( inputs ) );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" + input.algCompareString() + "&";
    }

}

