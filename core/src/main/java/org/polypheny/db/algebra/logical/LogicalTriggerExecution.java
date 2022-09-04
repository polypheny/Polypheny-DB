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
 */

package org.polypheny.db.algebra.logical;


import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.TriggerExecution;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;

import java.util.ArrayList;
import java.util.List;


/**
 * Sub-class of {@link TriggerExecution} not targeted at any particular engine or calling convention.
 */
public final class LogicalTriggerExecution extends TriggerExecution {

    private final LogicalTableModify modify;
    /**
     * Creates a LogicalTriggerExecution.
     * <p>
     * Use {@link #create} unless you know what you're doing.
     */
    public LogicalTriggerExecution(AlgOptCluster cluster, LogicalTableModify modify, AlgTraitSet traitSet, List<AlgNode> inputs, boolean all ) {
        super( cluster, traitSet, inputs, all );
        this.modify = modify;
    }


    /**
     * Creates a LogicalTriggerExecution.
     */
    public static LogicalTriggerExecution create(AlgOptCluster cluster, LogicalTableModify modify, List<AlgNode> inputs, boolean all ) {
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        List<AlgNode> copiedInputs = copyInputsWithCluster(cluster, inputs);
        return new LogicalTriggerExecution( cluster, modify, traitSet, copiedInputs, all );
    }

    private static List<AlgNode> copyInputsWithCluster(AlgOptCluster cluster, List<AlgNode> inputs) {
        List<AlgNode> inputCopy = new ArrayList<>();
        for(AlgNode node : inputs) {
            List<AlgNode> subNodeCopies = new ArrayList<>();
            for(AlgNode subNode : node.getInputs()) {
                AlgNode subNodeCopy = overwriteCluster(cluster, subNode);
                subNodeCopies.add(subNodeCopy);
            }
            AlgNode nodeCopy = overwriteCluster(cluster, node, subNodeCopies);
            inputCopy.add(nodeCopy);
        }
        return inputCopy;
    }

    private static AlgNode overwriteCluster(AlgOptCluster cluster, AlgNode node) {
        return overwriteCluster(cluster, node, node.getInputs());
    }

    private static AlgNode overwriteCluster(AlgOptCluster cluster, AlgNode node, List<AlgNode> inputs) {
        AlgNode nodeCopy = node.copy(node.getTraitSet(), inputs);
        ((AbstractAlgNode) nodeCopy).setCluster(cluster);
        return nodeCopy;
    }


    @Override
    public LogicalTriggerExecution copy(AlgTraitSet traitSet, List<AlgNode> inputs, boolean all ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new LogicalTriggerExecution( getCluster(), modify, traitSet, inputs, all );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

    public LogicalTableModify getModify() {
        return modify;
    }
}

