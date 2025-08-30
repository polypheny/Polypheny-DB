/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.routing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.polyalg.PolyAlgMetadata.GlobalStats;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.information.InformationPolyAlg.PlanType;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.processing.util.Plan;
import org.polypheny.db.transaction.QueryAnalyzer.StatementAnalyzer;
import org.polypheny.db.transaction.Statement;


/**
 * Adds debug information from routing to the ui.
 */
@Slf4j
public class UiRoutingPageUtil {

    private static final int RUNNERS = 1;
    private static final ExecutorService executorService = Executors.newFixedThreadPool( RUNNERS );
    private static final AtomicInteger counter = new AtomicInteger( 0 );


    public static int runningTasks() {
        return counter.get();
    }


    public static void outputSingleResult( Plan plan, StatementAnalyzer analyzer, boolean attachTextualPlan ) {
        addPhysicalPlanPage( plan.optimalNode(), analyzer, attachTextualPlan );
        setBaseOutput( 1, plan.proposedRoutingPlan(), analyzer );
        analyzer.registerSelectedAdapterTable( plan.proposedRoutingPlan().getRoutedDistribution() );
        final AlgRoot root = plan.proposedRoutingPlan().getRoutedRoot();
        addRoutedPolyPlanPage( root.alg, analyzer, false, attachTextualPlan );
    }


    public static void addPhysicalPlanPage( AlgNode optimalNode, StatementAnalyzer analyzer, boolean attachTextualPlan ) {
        counter.incrementAndGet();
        executorService.submit( () -> {
            try {
                addRoutedPolyPlanPage( optimalNode, analyzer, true, attachTextualPlan );
            } catch ( Throwable t ) {
                log.error( "Error adding routing plan", t );
            }
            counter.decrementAndGet();
        } );

    }


    private static void addRoutedPolyPlanPage( AlgNode routedNode, StatementAnalyzer analyzer, boolean isPhysical, boolean attachTextualPlan ) {
        ObjectMapper objectMapper = new ObjectMapper();
        GlobalStats gs = GlobalStats.computeGlobalStats( routedNode );

        ObjectNode objectNode = routedNode.serializePolyAlgebra( objectMapper, gs );
        String jsonString;
        try {
            jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString( objectNode );
        } catch ( JsonProcessingException e ) {
            throw new GenericRuntimeException( e );
        }

        String serialized = null;
        if ( attachTextualPlan ) {
            serialized = routedNode.buildPolyAlgebra( (String) null );
            if ( serialized == null ) {
                throw new GenericRuntimeException( "Unable to serialize routing plan" );
            }
        }
        analyzer.registerQueryPlan( isPhysical ? PlanType.PHYSICAL : PlanType.ALLOCATION, jsonString, serialized );
    }


    public static void setBaseOutput( Integer numberOfPlans, RoutingPlan selectedPlan, StatementAnalyzer analyzer ) {
        double ratioPre = 1 - RoutingManager.PRE_COST_POST_COST_RATIO.getDouble();
        double ratioPost = RoutingManager.PRE_COST_POST_COST_RATIO.getDouble();
        analyzer.registerRoutingBaseOutput( numberOfPlans, selectedPlan, ratioPre, ratioPost, RoutingManager.PLAN_SELECTION_STRATEGY.getEnum() );
    }


    public static void addRoutingAndPlanPage(
            List<AlgOptCost> approximatedCosts,
            List<Double> preCosts,
            List<Double> postCosts,
            List<Double> icarusCosts,
            List<? extends RoutingPlan> routingPlans,
            RoutingPlan selectedPlan,
            List<Double> effectiveCosts,
            List<Double> percentageCosts,
            Statement statement ) {
        StatementAnalyzer analyzer = statement.getAnalyzer();
        setBaseOutput( routingPlans.size(), selectedPlan, analyzer );
        analyzer.registerProposedPlans( approximatedCosts, preCosts, postCosts, icarusCosts, routingPlans, effectiveCosts, percentageCosts );

        if ( selectedPlan instanceof ProposedRoutingPlan plan ) {
            analyzer.registerSelectedAdapterTable( plan.getRoutedDistribution() );
            AlgRoot root = plan.getRoutedRoot();
            addRoutedPolyPlanPage( root.alg, statement.getAnalyzer(), false, statement.getTransaction().getOrigin().equals( "PolyAlgParsingTest" ) );
        }

    }

}
