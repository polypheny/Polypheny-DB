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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
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
public class UiRoutingPageUtil {

    public static void outputSingleResult( ProposedRoutingPlan proposedRoutingPlan, RelNode optimalRelNode, InformationManager queryAnalyzer ) {
        addPhysicalPlanPage( optimalRelNode, queryAnalyzer );

        InformationPage page = setBaseOutput( "Routing", 0, queryAnalyzer );
        addSelectedAdapterTable( queryAnalyzer, proposedRoutingPlan, page );
        final RelRoot root = proposedRoutingPlan.getRoutedRoot();
        addRoutedPlanPage( root.rel, queryAnalyzer );
    }


    public static void addPhysicalPlanPage( RelNode optimalNode, InformationManager queryAnalyzer ) {
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


    private static void addRoutedPlanPage( RelNode routedNode, InformationManager queryAnalyzer ) {
        InformationPage page = new InformationPage( "Routed Query Plan" ).setLabel( "plans" );
        page.fullWidth();
        InformationGroup group = new InformationGroup( page, "Routed Query Plan" );
        queryAnalyzer.addPage( page );
        queryAnalyzer.addGroup( group );
        InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                group,
                RelOptUtil.dumpPlan( "Routed Query Plan", routedNode, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
        queryAnalyzer.registerInformation( informationQueryPlan );
    }


    private static void addSelectedAdapterTable( InformationManager queryAnalyzer, ProposedRoutingPlan proposedRoutingPlan, InformationPage page ) {
        InformationGroup group = new InformationGroup( page, "Selected Adapters" );
        queryAnalyzer.addGroup( group );
        InformationTable table = new InformationTable(
                group,
                ImmutableList.of( "Table", "Adapter", "Physical Name" ) );
        proposedRoutingPlan.getSelectedAdaptersInfo().forEach( ( k, v ) -> {
            CatalogTable catalogTable = Catalog.getInstance().getTable( k );
            table.addRow( catalogTable.getSchemaName() + "." + catalogTable.name, v.uniqueName, v.physicalSchemaName + "." + v.physicalTableName );
        } );
        queryAnalyzer.registerInformation( table );
    }


    public static InformationPage setBaseOutput( String title, Integer numberOfPlans, InformationManager queryAnalyzer ) {
        InformationPage page = new InformationPage( title );
        page.fullWidth();
        queryAnalyzer.addPage( page );

        double ratioPre = 1 - RoutingManager.PRE_COST_POST_COST_RATIO.getDouble();
        double ratioPost = RoutingManager.PRE_COST_POST_COST_RATIO.getDouble();

        InformationGroup overview = new InformationGroup( page, "Overview" ).setOrder( 1 );
        queryAnalyzer.addGroup( overview );
        InformationTable overviewTable = new InformationTable( overview, ImmutableList.of( "# of Plans", "Pre Cost Factor", "Post Cost Factor", "Selection Strategy" ) );
        overviewTable.addRow( numberOfPlans == 0 ? "-" : numberOfPlans, ratioPre, ratioPost, RoutingManager.PLAN_SELECTION_STRATEGY.getEnum() );
        queryAnalyzer.registerInformation( overviewTable );

        return page;
    }


    public static void addRoutingAndPlanPage(
            List<RelOptCost> approximatedCosts,
            List<Double> preCosts,
            List<Double> postCosts,
            List<Double> icarusCosts,
            List<? extends RoutingPlan> routingPlans,
            RoutingPlan selectedPlan,
            List<Double> effectiveCosts,
            List<Double> percentageCosts,
            Statement statement ) {

        InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
        InformationPage page = setBaseOutput( "Routing", routingPlans.size(), queryAnalyzer );

        final boolean isIcarus = icarusCosts != null;

        InformationGroup group = new InformationGroup( page, "Proposed Plans" ).setOrder( 2 );
        queryAnalyzer.addGroup( group );
        InformationTable table = new InformationTable(
                group,
                ImmutableList.of( "Physical Query Class", "Router", "Pre. Costs", "Post Costs", "Norm. pre Costs", "Norm. post Costs", "Total Costs", "Adapter Info", "Percentage" ) );

        for ( int i = 0; i < routingPlans.size(); i++ ) {
            final RoutingPlan routingPlan = routingPlans.get( i );
            table.addRow(
                    routingPlan.getPhysicalQueryClass(),
                    routingPlan.getRouter() != null ? routingPlan.getRouter().toString().replace( "class org.polypheny.db.routing.routers.", "" ) : "",
                    approximatedCosts.get( i ),
                    isIcarus ? icarusCosts.get( i ) : "-",
                    preCosts.get( i ),
                    isIcarus ? postCosts.get( i ) : "-",
                    effectiveCosts.get( i ),
                    routingPlan.getPhysicalPlacementsOfPartitions(),
                    percentageCosts != null ? percentageCosts.get( i ) : "-" );
        }

        InformationGroup selected = new InformationGroup( page, "Selected Plan" ).setOrder( 3 );
        queryAnalyzer.addGroup( selected );
        InformationTable selectedTable = new InformationTable(
                selected,
                ImmutableList.of( "Query Class", "Physical Query Class", "Router", "Partitioning - Placements" ) );
        selectedTable.addRow(
                selectedPlan.getQueryClass(),
                selectedPlan.getPhysicalQueryClass(),
                selectedPlan.getRouter().toString().replace( "class org.polypheny.db.routing.routers.", "" ),
                selectedPlan.getPhysicalPlacementsOfPartitions() );

        queryAnalyzer.registerInformation( table, selectedTable );

        if ( selectedPlan instanceof ProposedRoutingPlan ) {
            ProposedRoutingPlan plan = (ProposedRoutingPlan) selectedPlan;
            addSelectedAdapterTable( queryAnalyzer, plan, page );
            RelRoot root = plan.getRoutedRoot();
            addRoutedPlanPage( root.rel, queryAnalyzer );
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

    }

}
