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

package org.polypheny.db.routing.strategies;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.routing.RouterPlanSelectionStrategy;
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.routing.RoutingPlan;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Pair;

/**
 * Class which is responsible for cost calculation and plan selection.
 */
@Slf4j
public class RoutingPlanSelector {


    public Pair<Optional<PolyphenyDbSignature<?>>, RoutingPlan> selectPlanBasedOnCosts( List<? extends RoutingPlan> routingPlans, List<RelOptCost> approximatedCosts, String queryId, List<PolyphenyDbSignature<?>> signatures, Statement statement ) {
        return this.selectPlanBasedOnCosts( routingPlans, approximatedCosts, queryId, Optional.of( signatures ), statement );
    }


    public Pair<Optional<PolyphenyDbSignature<?>>, RoutingPlan> selectPlanBasedOnCosts( List<? extends RoutingPlan> routingPlans, List<RelOptCost> approximatedCosts, String queryId, Statement statement ) {
        return this.selectPlanBasedOnCosts( routingPlans, approximatedCosts, queryId, Optional.empty(), statement );
    }


    public Pair<Optional<PolyphenyDbSignature<?>>, RoutingPlan> selectPlanBasedOnCosts( List<? extends RoutingPlan> routingPlans, List<RelOptCost> approximatedCosts, String queryId, Optional<List<PolyphenyDbSignature<?>>> signatures, Statement statement ) {
        // 0 = only consider pre costs
        // 1 = only consider post costs
        // [0,1] = get normalized pre and post costs and multiply by ratio
        final double ratioPre = 1 - RoutingManager.PRE_COST_POST_COST_RATIO.getDouble();
        final double ratioPost = RoutingManager.PRE_COST_POST_COST_RATIO.getDouble();
        final int n = routingPlans.size();

        // calc pre-costs or set them to 0 for all entries.
        boolean calcPreCosts = Math.abs( ratioPre ) >= RelOptUtil.EPSILON; // <=0
        List<Double> preCosts = calcPreCosts ? normalizeApproximatedCosts( approximatedCosts ) : Collections.nCopies( n, 0.0 );

        // calc post-costs or set them to 0 for all entries.
        // If calculation is needed, get icarus original costs for printing debug output.
        boolean calcPostCosts = Math.abs( ratioPost ) >= RelOptUtil.EPSILON; // > 0
        Optional<List<Double>> icarusCosts;
        List<Double> postCosts;
        Optional<List<Double>> percentageCosts = Optional.empty();
        if ( calcPostCosts ) {
            Pair<List<Double>, List<Double>> icarusResult = calcIcarusPostCosts( routingPlans, queryId );
            icarusCosts = Optional.of( icarusResult.right );
            postCosts = icarusResult.left;
        } else {
            postCosts = Collections.nCopies( n, 0.0 );
            icarusCosts = Optional.empty();
        }

        // Get effective costs
        Pair<Optional<PolyphenyDbSignature<?>>, RoutingPlan> result = null;
        List<Double> effectiveCosts = IntStream.rangeClosed( 0, n - 1 )
                .mapToDouble( i -> (preCosts.get( i ) * ratioPre) + (postCosts.get( i ) * ratioPost) )
                .boxed()
                .collect( Collectors.toList() );

        // Get plan in regard to the active strategy
        if ( RoutingManager.PLAN_SELECTION_STRATEGY.getEnum() == RouterPlanSelectionStrategy.BEST ) {
            result = this.selectBestPlan( routingPlans, signatures, effectiveCosts );
        } else if ( RoutingManager.PLAN_SELECTION_STRATEGY.getEnum() == RouterPlanSelectionStrategy.PROBABILITY ) {
            Pair<Pair<Optional<PolyphenyDbSignature<?>>, RoutingPlan>, List<Double>> percentageResult = this.selectPlanFromProbability( routingPlans, signatures, effectiveCosts );
            result = percentageResult.left;
            percentageCosts = Optional.of( percentageResult.right );
        }

        if ( result == null ) {
            throw new RuntimeException( "Plan selection strategy not found, result still null." );
        }

        if ( statement.getTransaction().isAnalyze() ) {
            RoutingManager.getInstance().getDebugUiPrinter().printDebugOutput( approximatedCosts, preCosts, postCosts, icarusCosts, routingPlans, result.right, effectiveCosts, percentageCosts, statement );
        }

        return result;
    }


    private Pair<Pair<Optional<PolyphenyDbSignature<?>>, RoutingPlan>, List<Double>> selectPlanFromProbability( List<? extends RoutingPlan> routingPlans, Optional<List<PolyphenyDbSignature<?>>> signatures, List<Double> effectiveCosts ) {
        // Check for list all 0
        if ( effectiveCosts.stream().allMatch( value -> value <= RelOptUtil.EPSILON ) ) {
            effectiveCosts = Collections.nCopies( effectiveCosts.size(), 1.0 );
        }

        // Get probabilities
        final List<Double> probabilities = calculateProbabilities( effectiveCosts );
        final List<Double> inverseProbabilities = calculateInverseProbabilities( probabilities );

        // Select plan based on percentages
        double p = 0.0;
        // Could be not 1 due to rounding errors
        double totalWeights = inverseProbabilities.stream().mapToDouble( Double::doubleValue ).sum();
        double random = (Math.random() * totalWeights);

        // Iterate over percentages and get plan
        for ( int i = 0; i < inverseProbabilities.size(); i++ ) {
            double weight = inverseProbabilities.get( i );
            p += weight;
            if ( p >= random ) {
                return new Pair( new Pair( signatures.isPresent() ? Optional.of( signatures.get().get( 0 ) ) : Optional.empty(), routingPlans.get( i ) ), inverseProbabilities );
            }
        }

        log.error( "Should never happen from percentages" );
        return new Pair( new Pair( signatures.isPresent() ? Optional.of( signatures.get().get( 0 ) ) : Optional.empty(), routingPlans.get( 0 ) ), inverseProbabilities );
    }


    private Pair<Optional<PolyphenyDbSignature<?>>, RoutingPlan> selectBestPlan( List<? extends RoutingPlan> routingPlans, Optional<List<PolyphenyDbSignature<?>>> signatures, List<Double> effectiveCosts ) {
        RoutingPlan currentPlan = routingPlans.get( 0 );
        Optional<PolyphenyDbSignature<?>> currentSignature = signatures.map( polyphenyDbSignatures -> polyphenyDbSignatures.get( 0 ) );
        Double currentCost = effectiveCosts.get( 0 );

        for ( int i = 1; i < routingPlans.size(); i++ ) {
            final double cost = effectiveCosts.get( i );
            if ( cost < currentCost ) {
                currentPlan = routingPlans.get( i );
                currentSignature = signatures.isPresent() ? Optional.of( signatures.get().get( i ) ) : Optional.empty();
                currentCost = cost;
            }
        }

        return new Pair<>( currentSignature, currentPlan );
    }


    private Pair<List<Double>, List<Double>> calcIcarusPostCosts( List<? extends RoutingPlan> proposedRoutingPlans, String queryId ) {
        final List<Long> postCosts = proposedRoutingPlans.stream()
                .map( plan -> MonitoringServiceProvider.getInstance().getQueryPostCosts( plan.getPhysicalQueryClass() ).getExecutionTime() )
                .collect( Collectors.toList() );

        // Check null values in icarus special case
        final boolean checkNullValues = RoutingManager.PRE_COST_POST_COST_RATIO.getDouble() >= 1;
        final List<Double> icarusCosts = postCosts.stream().map( value -> {
            // Fallback for pure icarus routing, when no time is available.
            if ( checkNullValues && value == 0 ) {
                return 1.0;
            } else {
                return (double) value;
            }
        } ).collect( Collectors.toList() );

        // Normalize values
        List<Double> normalized = calculateProbabilities( icarusCosts );
        return new Pair<>( normalized, icarusCosts );
    }


    private List<Double> calculateProbabilities( List<Double> input ) {
        // Check all zero
        if ( input.stream().allMatch( value -> value <= RelOptUtil.EPSILON ) ) {
            return input;
        }

        // Calc probabilities
        double hundredPercent = input.stream().mapToDouble( Double::doubleValue ).sum();
        return input.stream().map( cost -> cost / hundredPercent ).collect( Collectors.toList() );
    }


    private List<Double> calculateInverseProbabilities( List<Double> percentage ) {
        // Invert probabilities
        final List<Double> inverseProbabilityPart = percentage.stream()
                .map( value -> 1.0 / value )
                .map( value -> Double.isInfinite( value ) ? 0.0 : value )
                .collect( Collectors.toList() );

        final double totalInverseProbability = inverseProbabilityPart.stream()
                .mapToDouble( Double::doubleValue )
                .sum();

        // Normalize again
        return inverseProbabilityPart.stream()
                .map( value -> (value / totalInverseProbability) * 100.0 )
                .collect( Collectors.toList() );
    }


    private List<Double> normalizeApproximatedCosts( List<RelOptCost> approximatedCosts ) {
        final List<Double> costs = approximatedCosts.stream().map( RelOptCost::getCosts ).collect( Collectors.toList() );
        return calculateProbabilities( costs );
    }


    /**
     * Not used so far, could be added as normalization strategy.
     */
    private List<Double> normalizeMinMax( List<Double> costs ) {
        // Check all zero
        if ( costs.stream().allMatch( value -> value <= RelOptUtil.EPSILON ) ) {
            return costs;
        }

        Optional<Double> min = costs.stream().min( Double::compareTo );
        Optional<Double> max = costs.stream().max( Double::compareTo );

        // when min == mix, set min = 0
        Optional<Double> usedMin = Math.abs( min.get() - max.get() ) <= RelOptUtil.EPSILON ? Optional.of( 0.0 ) : min;
        return costs.stream()
                .map( c -> (c - usedMin.get()) / (max.get() - usedMin.get()) )
                .collect( Collectors.toList() );
    }

}
