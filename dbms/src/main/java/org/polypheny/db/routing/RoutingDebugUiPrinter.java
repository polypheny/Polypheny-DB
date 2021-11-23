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

package org.polypheny.db.routing;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.sql.SqlExplainFormat;
import org.polypheny.db.sql.SqlExplainLevel;
import org.polypheny.db.transaction.Statement;

/**
 * Adds debug information from routing to the ui.
 */
public class RoutingDebugUiPrinter {

    public void printDebugOutputSingleResult( ProposedRoutingPlan proposedRoutingPlan, RelNode optimalRelNode, Statement statement ) {
        // Print stuff in reverse order
        this.printExecutedPhysicalPlan( optimalRelNode, statement );

        InformationPage page = this.printBaseOutput( "Routed Query Plan", 0, statement );

        InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
        if ( proposedRoutingPlan != null ) {
            final RelRoot root = proposedRoutingPlan.getRoutedRoot();
            InformationGroup routedQueryPlan = new InformationGroup( page, "Routed Query Plan" );
            queryAnalyzer.addGroup( routedQueryPlan );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    routedQueryPlan,
                    RelOptUtil.dumpPlan( "Routed Query Plan", root.rel, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        }
    }


    public void printExecutedPhysicalPlan( RelNode optimalNode, Statement statement ) {
        if ( statement.getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "Physical Query Plan" ).setLabel( "plans" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Physical Query Plan" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    group,
                    RelOptUtil.dumpPlan( "Physical Query Plan", optimalNode, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        }
    }


    public InformationPage printBaseOutput( String title, Integer numberOfPlans, Statement statement ) {
        InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
        InformationPage page = new InformationPage( title );
        page.fullWidth();
        queryAnalyzer.addPage( page );

        double ratioPre = 1 - RoutingManager.PRE_COST_POST_COST_RATIO.getDouble();
        double ratioPost = RoutingManager.PRE_COST_POST_COST_RATIO.getDouble();

        InformationGroup overview = new InformationGroup( page, "Stats overview" );
        queryAnalyzer.addGroup( overview );
        InformationTable overviewTable = new InformationTable( overview, ImmutableList.of( "# of Plans", "Pre Cost factor", "Post cost factor", "Selection Strategy" ) );
        overviewTable.addRow( numberOfPlans == 0 ? "-" : numberOfPlans, ratioPre, ratioPost, RoutingManager.PLAN_SELECTION_STRATEGY.getEnum() );

        queryAnalyzer.registerInformation( overviewTable );

        return page;
    }


    public void printDebugOutput(
            List<RelOptCost> approximatedCosts,
            List<Double> preCosts,
            List<Double> postCosts,
            Optional<List<Double>> icarusCosts,
            List<? extends RoutingPlan> routingPlans,
            RoutingPlan selectedPlan,
            List<Double> effectiveCosts,
            Optional<List<Double>> percentageCosts,
            Statement statement ) {

        InformationPage page = this.printBaseOutput( "Routing", routingPlans.size(), statement );
        InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();

        final boolean isIcarus = icarusCosts.isPresent();

        InformationGroup group = new InformationGroup( page, "Proposed Plans" );
        queryAnalyzer.addGroup( group );
        InformationTable table = new InformationTable(
                group,
                ImmutableList.of( "Physical QueryClass", "router", "Pre. Costs", "Post Costs", "Norm. pre Costs", "Norm. post Costs", "Total Costs", "Adapter Info", "Percentage" ) );

        for ( int i = 0; i < routingPlans.size(); i++ ) {
            final RoutingPlan routingPlan = routingPlans.get( i );
            table.addRow(
                    routingPlan.getPhysicalQueryClass(),
                    routingPlan.getRouter().isPresent() ? routingPlan.getRouter().get() : "",
                    approximatedCosts.get( i ),
                    isIcarus ? icarusCosts.get().get( i ) : "-",
                    preCosts.get( i ),
                    isIcarus ? postCosts.get( i ) : "-",
                    effectiveCosts.get( i ),
                    routingPlan.getOptionalPhysicalPlacementsOfPartitions(),
                    percentageCosts.isPresent() ? percentageCosts.get().get( i ) : "-" );
        }

        InformationGroup selected = new InformationGroup( page, "Selected Plan" );
        queryAnalyzer.addGroup( selected );
        InformationTable selectedTable = new InformationTable( selected, ImmutableList.of( "QueryClass", "Physical QueryClass", "Router", "Partitioning - Placements" ) );
        selectedTable.addRow(
                selectedPlan.getQueryClass(),
                selectedPlan.getPhysicalQueryClass(),
                selectedPlan.getRouter(),
                selectedPlan.getOptionalPhysicalPlacementsOfPartitions() );

        if ( selectedPlan instanceof ProposedRoutingPlan ) {
            ProposedRoutingPlan plan = (ProposedRoutingPlan) selectedPlan;
            RelRoot root = plan.getRoutedRoot();
            if ( statement.getTransaction().isAnalyze() ) {
                InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                        selected,
                        RelOptUtil.dumpPlan( "Routed Query Plan", root.rel, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
                queryAnalyzer.registerInformation( informationQueryPlan );
            }
        }

        /*val plans = routingPlans.stream().map( elem -> elem instanceof ProposedRoutingPlan ? (ProposedRoutingPlan) elem : null ).collect( Collectors.toList() );
        for ( int i = 0; i < plans.size(); i++ ) {
            val proposedPlan = plans.get( i );
            if ( proposedPlan != null ) {
                val root = proposedPlan.getRoutedRoot();
                if ( statement.getTransaction().isAnalyze() ) {
                    InformationGroup physicalQueryPlan = new InformationGroup( page, "Routed Query Plan-" + i );
                    queryAnalyzer.addGroup( physicalQueryPlan );
                    InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                            physicalQueryPlan,
                            RelOptUtil.dumpPlan( "Routed Query Plan-" + i, root.rel, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
                    queryAnalyzer.registerInformation( informationQueryPlan );
                }
            }
        }*/

        queryAnalyzer.registerInformation( table, selectedTable );
    }

}
