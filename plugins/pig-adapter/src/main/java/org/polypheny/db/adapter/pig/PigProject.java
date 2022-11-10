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

package org.polypheny.db.adapter.pig;


import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;


/**
 * Implementation of {@link Project} in {@link PigAlg#CONVENTION Pig calling convention}.
 */
public class PigProject extends Project implements PigAlg {

    /**
     * Creates a PigProject.
     */
    public PigProject( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode input, List<? extends RexNode> projects, AlgDataType rowType ) {
        super( cluster, traitSet, input, projects, rowType );
        assert getConvention() == CONVENTION;
    }


    @Override
    public Project copy( AlgTraitSet traitSet, AlgNode input, List<RexNode> projects, AlgDataType rowType ) {
        return new PigProject( input.getCluster(), traitSet, input, projects, rowType );
    }


    @Override
    public void implement( Implementor implementor ) {
        System.out.println( getTable() );
    }


    /**
     * Override this method so it looks down the tree to find the table this node is acting on.
     */
    @Override
    public AlgOptTable getTable() {
        return getInput().getTable();
    }

}

