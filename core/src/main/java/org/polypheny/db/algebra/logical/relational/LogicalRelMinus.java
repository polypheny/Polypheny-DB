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

package org.polypheny.db.algebra.logical.relational;


import java.util.List;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.Minus;
import org.polypheny.db.algebra.core.relational.RelAlg;
import org.polypheny.db.algebra.polyalg.arguments.BooleanArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;


/**
 * Sub-class of {@link Minus} not targeted at any particular engine or calling convention.
 */
public final class LogicalRelMinus extends Minus implements RelAlg {

    /**
     * Creates a LogicalMinus.
     *
     * Use {@link #create} unless you know what you're doing.
     */
    public LogicalRelMinus( AlgCluster cluster, AlgTraitSet traitSet, List<AlgNode> inputs, boolean all ) {
        super( cluster, traitSet, inputs, all );
    }


    /**
     * Creates a LogicalMinus by parsing serialized output.
     */
    public LogicalRelMinus( AlgInput input ) {
        super( input );
    }


    /**
     * Creates a LogicalMinus.
     */
    public static LogicalRelMinus create( List<AlgNode> inputs, boolean all ) {
        final AlgCluster cluster = inputs.get( 0 ).getCluster();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalRelMinus( cluster, traitSet, inputs, all );
    }


    @Override
    public LogicalRelMinus copy( AlgTraitSet traitSet, List<AlgNode> inputs, boolean all ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalRelMinus( getCluster(), traitSet, inputs, all );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    @Override
    public PolyAlgArgs collectAttributes() {
        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );
        return args.put( "all", new BooleanArg( all ) );
    }

}

