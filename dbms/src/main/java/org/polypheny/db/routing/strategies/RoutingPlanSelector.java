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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.QueryDataPoint;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.routing.RouterManager;
import org.polypheny.db.routing.RouterPlanSelectionStrategy;
import org.polypheny.db.routing.RoutingPlan;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Pair;

@Slf4j
public class RoutingPlanSelector {


    public Pair<Optional<PolyphenyDbSignature>, RoutingPlan> selectPlanBasedOnCosts( List<? extends RoutingPlan> routingPlans, List<RelOptCost> approximatedCosts, String queryId, List<PolyphenyDbSignature> signatures, Statement statement ) {
        return this.selectPlanBasedOnCosts( routingPlans, approximatedCosts, queryId, Optional.of( signatures ), statement );
    }


    public Pair<Optional<PolyphenyDbSignature>, RoutingPlan> selectPlanBasedOnCosts( List<? extends RoutingPlan> routingPlans, List<RelOptCost> approximatedCosts, String queryId, Statement statement ) {
        return this.selectPlanBasedOnCosts( routingPlans, approximatedCosts, queryId, Optional.empty(), statement );
    }


    public Pair<Optional<PolyphenyDbSignature>, RoutingPlan> selectPlanBasedOnCosts( List<? extends RoutingPlan> routingPlans, List<RelOptCost> approximatedCosts, String queryId, Optional<List<PolyphenyDbSignature>> signatures, Statement statement ) {
        // 0 = only consider pre costs
        // 1 = only consider post costs
        // [0,1] = get normalized pre and post costs and multiply by ratio
        val ratioPre = 1 - RouterManager.PRE_COST_POST_COST_RATIO.getDouble();
        val ratioPost = RouterManager.PRE_COST_POST_COST_RATIO.getDouble();
        val n = routingPlans.size();

        // calc pre-costs or set them to 0 for all entries.
        val calcPreCosts = Math.abs( ratioPre ) >= RelOptUtil.EPSILON; // <=0
        List<Double> preCosts = calcPreCosts ? normalizeApproximatedCosts( approximatedCosts ) : Collections.nCopies( n, 0.0 );

        // calc post-costs or set them to 0 for all entries.
        // If calculation is needed, get icarus original costs for printing debug output.
        val calcPostCosts = Math.abs( ratioPost ) >= RelOptUtil.EPSILON; // > 0
        Optional<List<Double>> icarusCosts;
        List<Double> postCosts;
        Optional<List<Double>> percentageCosts = Optional.empty();
        if ( calcPostCosts ) {
            val icarusResult = calcIcarusPostCosts( routingPlans, queryId );
            icarusCosts = Optional.of( icarusResult.right );
            postCosts = icarusResult.left;
        } else {
            postCosts = Collections.nCopies( n, 0.0 );
            icarusCosts = Optional.empty();
        }

        // get effective costs
        Pair<Optional<PolyphenyDbSignature>, RoutingPlan> result = null;
        val effectiveCosts = IntStream.rangeClosed( 0, n - 1 )
                .mapToDouble( i -> (preCosts.get( i ) * ratioPre) + (postCosts.get( i ) * ratioPost) )
                .boxed()
                .collect( Collectors.toList() );

        if ( RouterManager.PLAN_SELECTION_STRATEGY.getEnum() == RouterPlanSelectionStrategy.BEST ) {
            val bestResult = this.selectBestPlan( routingPlans, signatures, effectiveCosts );
            result = bestResult;
        } else if ( RouterManager.PLAN_SELECTION_STRATEGY.getEnum() == RouterPlanSelectionStrategy.PERCENTAGE ) {
            val percentageResult = this.selectPlanFromPercentage( routingPlans, signatures, effectiveCosts );
            result = percentageResult.left;
            percentageCosts = Optional.of( percentageResult.right );
        }

        if ( result == null ) {
            throw new RuntimeException( "Plan selection strategy not found, result still null." );
        }

        if ( statement.getTransaction().isAnalyze() ) {
            RouterManager.getInstance().getDebugUiPrinter().printDebugOutput( approximatedCosts, preCosts, postCosts, icarusCosts, routingPlans, result.right, effectiveCosts, percentageCosts, statement );
        }

        return result;
    }


    private Pair<Pair<Optional<PolyphenyDbSignature>, RoutingPlan>, List<Double>> selectPlanFromPercentage( List<? extends RoutingPlan> routingPlans, Optional<List<PolyphenyDbSignature>> signatures, List<Double> effectiveCosts ) {
        // check for list all 0
        if ( effectiveCosts.stream().allMatch( value -> value <= RelOptUtil.EPSILON ) ) {
            effectiveCosts = Collections.nCopies( effectiveCosts.size(), 1.0 );
        }

        // get percentages
        val percentage = calculatePercentage( effectiveCosts );

        val inversePercentage = calculateInversePercentage( percentage );

        // select plan based on percentages
        double p = 0.0;
        // could be not 1 due to rounding errors
        val totalWeights = inversePercentage.stream().mapToDouble( Double::doubleValue ).sum();
        double random = (Math.random() * totalWeights);

        // iterate over percentages and get plan
        for ( int i = 0; i < inversePercentage.size(); i++ ) {
            val weight = inversePercentage.get( i );
            p += weight;
            if ( p >= random ) {
                return new Pair(
                        new Pair( signatures.isPresent() ? Optional.of( signatures.get().get( 0 ) ) : Optional.empty(), routingPlans.get( i ) )
                        , inversePercentage );
            }
        }

        log.error( "should never happen from percentages" );
        return new Pair(
                new Pair( signatures.isPresent() ? Optional.of( signatures.get().get( 0 ) ) : Optional.empty(), routingPlans.get( 0 ) )
                , inversePercentage );
    }


    private Pair<Optional<PolyphenyDbSignature>, RoutingPlan> selectBestPlan( List<? extends RoutingPlan> routingPlans, Optional<List<PolyphenyDbSignature>> signatures, List<Double> effectiveCosts ) {
        var currentPlan = routingPlans.get( 0 );
        Optional<PolyphenyDbSignature> currentSignature = signatures.map( polyphenyDbSignatures -> polyphenyDbSignatures.get( 0 ) );
        var currentCost = effectiveCosts.get( 0 );

        for ( int i = 1; i < routingPlans.size(); i++ ) {
            val cost = effectiveCosts.get( i );
            if ( cost < currentCost ) {
                currentPlan = routingPlans.get( i );
                currentSignature = signatures.isPresent() ? Optional.of( signatures.get().get( i ) ) : Optional.empty();
                currentCost = cost;
            }
        }

        return new Pair<>( currentSignature, currentPlan );
    }


    private Pair<List<Double>, List<Double>> calcIcarusPostCosts( List<? extends RoutingPlan> proposedRoutingPlans, String queryId ) {
        val measuredData = MonitoringServiceProvider.getInstance().getQueryDataPoints( queryId );
        val icarusCosts = new ArrayList<Double>();

        for ( final RoutingPlan plan : proposedRoutingPlans ) {
            val dataPoints = measuredData.stream()
                    .filter( elem -> elem.getPhysicalQueryId().equals( plan.getPhysicalQueryId() ) )
                    .collect( Collectors.toList() );
            if ( dataPoints.isEmpty() ) {

                // fallback for pure icarus routing.
                if ( RouterManager.PRE_COST_POST_COST_RATIO.getDouble() >= 1 ) {
                    icarusCosts.add( 1.0 );
                } else {
                    icarusCosts.add( 0.0 );
                }
            } else {
                val value = dataPoints.stream()
                        .map( QueryDataPoint::getExecutionTime )
                        .mapToDouble( d -> d )
                        .average();

                if ( value.isPresent() ) {
                    icarusCosts.add( value.getAsDouble() );
                } else {
                    // fallback for pure icarus routing.
                    if ( RouterManager.PRE_COST_POST_COST_RATIO.getDouble() >= 1 ) {
                        icarusCosts.add( 1.0 );
                    } else {
                        icarusCosts.add( 0.0 );
                    }
                }
            }
        }

        // normalize values
        val normalized = calculatePercentage( icarusCosts );
        return new Pair<>( normalized, icarusCosts );
    }


    private List<Double> calculatePercentage( List<Double> input ) {
        // check all zero
        if ( input.stream().allMatch( value -> value <= RelOptUtil.EPSILON ) ) {
            return input;
        }

        // calc percentages
        double hundredPercent = input.stream().mapToDouble( Double::doubleValue ).sum();

        val percentage = input.stream().map( cost -> cost / hundredPercent ).collect( Collectors.toList() );

        return percentage;

    }


    private List<Double> calculateInversePercentage( List<Double> percentage ) {
        // invert percentages
        val inversePercentagePart = percentage.stream()
                .map( value -> 1.0 / value )
                .map( value -> Double.isInfinite( value ) ? 0.0 : value ).collect( Collectors.toList() );

        val totalInversePercentage = inversePercentagePart.stream().mapToDouble( Double::doubleValue ).sum();

        // normalize again
        val inversePercentage = inversePercentagePart.stream().map( value -> (value / totalInversePercentage) * 100.0 ).collect( Collectors.toList() );

        return inversePercentage;
    }


    private List<Double> normalizeApproximatedCosts( List<RelOptCost> approximatedCosts ) {
        val costs = approximatedCosts.stream().map( RelOptCost::getCosts ).collect( Collectors.toList() );
        return calculatePercentage( costs );
    }


    private List<Double> normalizeCots( List<Double> costs ) {
        // check all zero
        if ( costs.stream().allMatch( value -> value <= RelOptUtil.EPSILON ) ) {
            return costs;
        }

        val min = costs.stream().min( Double::compareTo );
        val max = costs.stream().max( Double::compareTo );

        // when min == mix, set min = 0
        val usedMin = Math.abs( min.get() - max.get() ) <= RelOptUtil.EPSILON ? Optional.of( 0.0 ) : min;
        return costs.stream().map( c -> (c - usedMin.get()) / (max.get() - usedMin.get()) ).collect( Collectors.toList() );
    }


}
