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
 */

package org.polypheny.db.routing;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.processing.util.Plan;
import org.polypheny.db.routing.ColumnDistribution.FullPartition;
import org.polypheny.db.routing.ColumnDistribution.PartialPartition;
import org.polypheny.db.routing.ColumnDistribution.RoutedDistribution;
import org.polypheny.db.transaction.Statement;


/**
 * Adds debug information from routing to the ui.
 */
@Slf4j
public class UiRoutingPageUtil {


    public static void outputSingleResult( Plan plan, InformationManager queryAnalyzer ) {
        addPhysicalPlanPage( plan.optimalNode(), queryAnalyzer );

        InformationPage page = queryAnalyzer.getPage( "routing" );
        if ( page == null ) {
            page = setBaseOutput( "Routing", 1, plan.proposedRoutingPlan(), queryAnalyzer );
        }
        addSelectedAdapterTable( queryAnalyzer, plan.proposedRoutingPlan(), page );
        final AlgRoot root = plan.proposedRoutingPlan().getRoutedRoot();
        addRoutedPlanPage( root.alg, queryAnalyzer );
    }


    public static void addPhysicalPlanPage( AlgNode optimalNode, InformationManager queryAnalyzer ) {
        new Thread( () -> {
            InformationPage page = new InformationPage( "Physical Query Plan" ).setLabel( "plans" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Physical Query Plan" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    group,
                    AlgOptUtil.dumpPlan( "Physical Query Plan", optimalNode, ExplainFormat.JSON, ExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        } ).start();
    }


    private static void addRoutedPlanPage( AlgNode routedNode, InformationManager queryAnalyzer ) {
        InformationPage page = new InformationPage( "Routed Query Plan" ).setLabel( "plans" );
        page.fullWidth();
        InformationGroup group = new InformationGroup( page, "Routed Query Plan" );
        queryAnalyzer.addPage( page );
        queryAnalyzer.addGroup( group );
        InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                group,
                AlgOptUtil.dumpPlan( "Routed Query Plan", routedNode, ExplainFormat.JSON, ExplainLevel.ALL_ATTRIBUTES ) );
        queryAnalyzer.registerInformation( informationQueryPlan );
    }


    private static void addSelectedAdapterTable( InformationManager queryAnalyzer, ProposedRoutingPlan proposedRoutingPlan, InformationPage page ) {
        InformationGroup group = new InformationGroup( page, "Selected Placements" );
        queryAnalyzer.addGroup( group );
        InformationTable table = new InformationTable(
                group,
                ImmutableList.of( "Entity", "Placement Id", "Adapter", "Allocation Id" ) );
        if ( proposedRoutingPlan.getRoutedDistribution() != null ) {
            RoutedDistribution distribution = proposedRoutingPlan.getRoutedDistribution();
            LogicalEntity entity = distribution.entity();

            for ( FullPartition partition : distribution.partitions() ) {
                if ( entity.unwrap( LogicalTable.class ).isPresent() ) {
                    for ( PartialPartition partial : partition.partials() ) {
                        table.addRow(
                                entity.getNamespaceName() + "." + entity.name,
                                partial.entity().placementId,
                                partial.entity().adapterId,
                                partial.entity().id );
                    }

                } else if ( entity.unwrap( LogicalCollection.class ).isPresent() ) {
                    log.warn( "Collection not supported for routing page ui." );
                } else if ( entity.unwrap( LogicalGraph.class ).isPresent() ) {
                    log.warn( "Graph not supported for routing page ui." );
                } else {
                    log.warn( "Error when adding to UI of proposed planner." );
                }

            }
        }
        queryAnalyzer.registerInformation( table );
    }


    public static InformationPage setBaseOutput( String title, Integer numberOfPlans, RoutingPlan selectedPlan, InformationManager queryAnalyzer ) {
        InformationPage page = new InformationPage( "routing", title, null );
        page.fullWidth();
        queryAnalyzer.addPage( page );

        double ratioPre = 1 - RoutingManager.PRE_COST_POST_COST_RATIO.getDouble();
        double ratioPost = RoutingManager.PRE_COST_POST_COST_RATIO.getDouble();

        InformationGroup overview = new InformationGroup( page, "Overview" ).setOrder( 1 );
        queryAnalyzer.addGroup( overview );

        InformationTable overviewTable = new InformationTable( overview, ImmutableList.of( "Query Class", selectedPlan.getQueryClass() ) );
        overviewTable.addRow( "# of Proposed Plans", numberOfPlans == 0 ? "-" : numberOfPlans );
        overviewTable.addRow( "Pre Cost Factor", ratioPre );
        overviewTable.addRow( "Post Cost Factor", ratioPost );
        overviewTable.addRow( "Selection Strategy", RoutingManager.PLAN_SELECTION_STRATEGY.getEnum() );
        if ( selectedPlan.getRoutedDistribution() != null ) {
            overviewTable.addRow( "Selected Plan", selectedPlan.getRoutedDistribution().toString() );
        }
        if ( selectedPlan.getRouter() != null ) {
            overviewTable.addRow( "Proposed By", selectedPlan.getRouter().getSimpleName() );
        }
        queryAnalyzer.registerInformation( overviewTable );

        return page;
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

        InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
        InformationPage page = queryAnalyzer.getPage( "routing" );
        if ( page == null ) {
            page = setBaseOutput( "Routing", routingPlans.size(), selectedPlan, queryAnalyzer );
        }

        final boolean isIcarus = icarusCosts != null;

        InformationGroup group = new InformationGroup( page, "Proposed Plans" ).setOrder( 2 );
        queryAnalyzer.addGroup( group );
        InformationTable proposedPlansTable = new InformationTable(
                group,
                ImmutableList.of( "Physical", "Router", "Pre. Costs", "Norm. Pre Costs", "Post Costs", "Norm. Post Costs", "Total Costs", "Percentage" ) ); //"Physical (Partition --> <Adapter, ColumnPlacement>)"

        for ( int i = 0; i < routingPlans.size(); i++ ) {
            final RoutingPlan routingPlan = routingPlans.get( i );
            proposedPlansTable.addRow(
                    routingPlan.getRoutedDistribution().toString(),
                    routingPlan.getRouter() != null ? routingPlan.getRouter().getSimpleName() : "",
                    approximatedCosts.get( i ),
                    Math.round( preCosts.get( i ) * 100.0 ) / 100.0,
                    isIcarus ? Math.round( icarusCosts.get( i ) * 100.0 ) / 100.0 : "-",
                    isIcarus ? Math.round( postCosts.get( i ) * 100.0 ) / 100.0 : "-",
                    Math.round( effectiveCosts.get( i ) * 100.0 ) / 100.0,
                    percentageCosts != null ? Math.round( percentageCosts.get( i ) * 100.0 ) / 100.0 + " %" : "-" );
        }
        queryAnalyzer.registerInformation( proposedPlansTable );

        if ( selectedPlan instanceof ProposedRoutingPlan plan ) {
            addSelectedAdapterTable( queryAnalyzer, plan, page );
            AlgRoot root = plan.getRoutedRoot();
            addRoutedPlanPage( root.alg, queryAnalyzer );
        }

    }

}
