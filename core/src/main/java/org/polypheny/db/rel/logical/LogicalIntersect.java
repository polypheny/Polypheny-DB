/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.rel.logical;


import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelInput;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelShuttle;
import org.polypheny.db.rel.core.Intersect;
import java.util.List;


/**
 * Sub-class of {@link org.polypheny.db.rel.core.Intersect} not targeted at any particular engine or calling convention.
 */
public final class LogicalIntersect extends Intersect {

    /**
     * Creates a LogicalIntersect.
     *
     * Use {@link #create} unless you know what you're doing.
     */
    public LogicalIntersect( RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
        super( cluster, traitSet, inputs, all );
    }


    /**
     * Creates a LogicalIntersect by parsing serialized output.
     */
    public LogicalIntersect( RelInput input ) {
        super( input );
    }


    /**
     * Creates a LogicalIntersect.
     */
    public static LogicalIntersect create( List<RelNode> inputs, boolean all ) {
        final RelOptCluster cluster = inputs.get( 0 ).getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalIntersect( cluster, traitSet, inputs, all );
    }


    @Override
    public LogicalIntersect copy( RelTraitSet traitSet, List<RelNode> inputs, boolean all ) {
        return new LogicalIntersect( getCluster(), traitSet, inputs, all );
    }


    @Override
    public RelNode accept( RelShuttle shuttle ) {
        return shuttle.visit( this );
    }
}
