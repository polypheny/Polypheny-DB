/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.plan;

import java.io.Serializable;
import java.util.function.Function;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;

@Slf4j
public class PhysicalPlan implements Serializable, Comparable<PhysicalPlan> {

    @Getter
    private final transient AlgNode root;

    @Getter
    private transient AlgOptCost systemCost;

    @Getter
    private transient AlgOptCost weightedCost;

    @Getter
    private double systemApproxCost;

    @Getter
    private double weightedApproxCost;

    @Getter
    private long estimatedExecutionTime;

    @Getter
    private long actualExecutionTime;

    @Getter
    private String jsonAlg;

    public PhysicalPlan setEstimatedExecutionTime( long estimatedExecutionTime ) {
        this.estimatedExecutionTime = estimatedExecutionTime;
        return this;
    }

    public PhysicalPlan setActualExecutionTime( long actualExecutionTime ) {
        this.actualExecutionTime = actualExecutionTime;
        return this;
    }

    private PhysicalPlan( AlgNode root ) {
        this.root = root;
    }


    public static PhysicalPlan fromAlg( AlgNode root ) {
        return new PhysicalPlan( root );
    }

    /**
     * Computes the cost of a physical plan.
     * @param planner       planner used.
     * @param estimator     estimator for plan costs.
     * @param weight        weight between costs computed by system and by estimator.
     * @return approximate cost of plan
     */
    public PhysicalPlan calculateCosts( AlgOptPlanner planner, Function<PhysicalPlan, Long> estimator, float weight ) {
        setEstimatedExecutionTime( estimator.apply( this ) );
        this.systemCost = this.root.computeSelfCost( planner, this.root.getCluster().getMetadataQuery() );
        AlgOptCost systemCostW = this.systemCost.multiplyBy( 1 - weight );
        this.weightedCost = systemCostW
                .plus( planner.getCostFactory().makeCost( getEstimatedExecutionTime(), systemCostW.getCpu(), systemCostW.getIo() ).multiplyBy( weight ) );
        return this;
    }

    /**
     * Called once for selected plan per query.
     */
    public PhysicalPlan getSerializable() {
        this.systemApproxCost = systemCost.getCosts();
        this.weightedApproxCost = weightedCost.getCosts();
        this.jsonAlg = AlgOptUtil.dumpPlan( "plan", this.root, ExplainFormat.JSON, ExplainLevel.ALL_ATTRIBUTES );
        return this;
    }

    @Override
    public int compareTo( PhysicalPlan o ) {
        return Long.compare( getEstimatedExecutionTime(), o.getEstimatedExecutionTime() );
    }


    @Override
    public String toString() {
        return "PhysicalPlan {" +
                ", estimatedExecutionTime=" + estimatedExecutionTime + "ms" +
                ", actualExecutionTime=" + actualExecutionTime + "ms" +
                ", difference=" + Math.abs(actualExecutionTime - estimatedExecutionTime) + "ms" +
                ", systemCost=" + systemApproxCost +
                ", weightedCost=" + weightedApproxCost +
                ", " + jsonAlg +
                '}';
    }

}
