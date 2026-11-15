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

package org.polypheny.db.transaction;

import com.google.common.collect.ImmutableList;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.information.Information;
import org.polypheny.db.information.InformationCode;
import org.polypheny.db.information.InformationDuration;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationObserver;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationPolyAlg;
import org.polypheny.db.information.InformationPolyAlg.PlanType;
import org.polypheny.db.information.InformationStacktrace;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.information.InformationText;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.routing.ColumnDistribution.FullPartition;
import org.polypheny.db.routing.ColumnDistribution.PartialPartition;
import org.polypheny.db.routing.ColumnDistribution.RoutedDistribution;
import org.polypheny.db.routing.RoutingPlan;

@Slf4j
public class QueryAnalyzer {

    private final InformationManager manager;
    private final List<Transaction> transactions = new ArrayList<>();
    private InformationPage transactionsPage;
    private InformationTable transactionsTable;


    public QueryAnalyzer() {
        this.manager = InformationManager.getInstance( "QueryAnalyzer-" + UUID.randomUUID() );
    }


    public void visitInformationTarget( Consumer<InformationManager> informationTarget ) {
        informationTarget.accept( manager );
    }


    public void observe( InformationObserver observer ) {
        manager.observe( observer );
    }


    public Information[] getInformationArray() {
        return manager.getInformationArray();
    }


    /**
     * Adds top-level information to the transactions page
     */
    public void registerOverview( long executionTime, int numberOfQueries ) {
        initTransactionsPage();

        InformationGroup g = new InformationGroup( transactionsPage, "Overview" ).setOrder( 1 );
        InformationText text1;
        if ( executionTime < 1e4 ) {
            text1 = new InformationText( g, String.format( "Execution time: %d nanoseconds", executionTime ) );
        } else {
            long millis = TimeUnit.MILLISECONDS.convert( executionTime, TimeUnit.NANOSECONDS );
            // format time: see: https://stackoverflow.com/questions/625433/how-to-convert-milliseconds-to-x-mins-x-seconds-in-java#answer-625444
            DateFormat df = new SimpleDateFormat( "m 'min' s 'sec' S 'ms'" );
            String durationText = df.format( new Date( millis ) );
            text1 = new InformationText( g, String.format( "Execution time: %s", durationText ) );
        }

        // Number of queries
        InformationText text2 = new InformationText( g, String.format( "Total number of statements: %d", numberOfQueries ) );
        manager.addGroup( g );
        manager.registerInformation( text1 );
        manager.registerInformation( text2 );
    }


    private void registerTransaction( Transaction transaction, String commitStatus ) {
        if ( transactions.contains( transaction ) ) {
            return; // already registered
        }
        transactions.add( transaction );

        if ( transactionsTable == null ) {
            initTransactionsPage();
            InformationGroup g = new InformationGroup( transactionsPage, "Transactions" ).setOrder( 2 );
            transactionsTable = new InformationTable( g, List.of( "Transaction", "Status", "# of Statements" ) );
            manager.addGroup( g );
            manager.registerInformation( transactionsTable );
        }
        transactionsTable.addRow( transactions.indexOf( transaction ) + 1, commitStatus, transaction.getNumberOfStatements() );
    }


    private void registerImplementation( Statement statement, String code ) {
        InformationPage page = new InformationPage( "Implementation" )
                .setStmtLabel( statement.getIndex() )
                .fullWidth();
        InformationGroup group = new InformationGroup( page, "Java Code" );

        // Clean Code (remove package names to make code better readable)
        String cleanedCode = code.replaceAll( "(org.)([a-z][a-z_0-9]*\\.)*", "" );

        Information informationCode = new InformationCode( group, cleanedCode );
        manager.addPage( page );
        manager.addGroup( group );
        manager.registerInformation( informationCode );
    }


    private void registerGeneratedCode( Statement statement, String code ) {
        // TODO: is its usage redundant since registerImplementation is also called?
        InformationPage page = new InformationPage( "Generated Code" )
                .setStmtLabel( statement.getIndex() )
                .fullWidth();
        InformationGroup group = new InformationGroup( page, "Generated Code" );
        InformationCode informationCode = new InformationCode( group, code );
        manager.addPage( page );
        manager.addGroup( group );
        manager.registerInformation( informationCode );
    }


    private void registerQueryPlan( Statement statement, PlanType planType, String jsonPolyAlg, @Nullable String textualPolyAlg ) {
        String title = planType.getDisplayName() + " Query Plan";
        InformationPage page = new InformationPage( title )
                .setStmtLabel( statement.getIndex() )
                .fullWidth();
        InformationGroup group = new InformationGroup( page, title );
        InformationPolyAlg infoPolyAlg = new InformationPolyAlg( group, jsonPolyAlg, planType );
        if ( textualPolyAlg != null ) {
            infoPolyAlg.setTextualPolyAlg( textualPolyAlg );
        }
        manager.addPage( page );
        manager.addGroup( group );
        manager.registerInformation( infoPolyAlg );
    }


    private void registerConstraintPlan( Statement statement, String jsonPolyAlg ) {
        InformationPage page = new InformationPage( "Constraint Enforcement Plan" )
                .setStmtLabel( statement.getIndex() )
                .fullWidth();
        InformationGroup group = new InformationGroup( page, "Constraint Enforcement Plan" );
        InformationPolyAlg infoPolyAlg = new InformationPolyAlg( group, jsonPolyAlg, PlanType.LOGICAL );
        manager.addPage( page );
        manager.addGroup( group );
        manager.registerInformation( infoPolyAlg );
    }


    private void registerException( Statement statement, Throwable exception ) {
        InformationPage exceptionPage = new InformationPage( "Stacktrace" )
                .setStmtLabel( statement.getIndex() )
                .fullWidth();
        InformationGroup exceptionGroup = new InformationGroup( exceptionPage.getId(), "Stacktrace" );
        InformationStacktrace exceptionElement = new InformationStacktrace( exception, exceptionGroup );
        manager.addPage( exceptionPage );
        manager.addGroup( exceptionGroup );
        manager.registerInformation( exceptionElement );
    }


    private InformationPage registerRoutingBaseOutput( Statement statement, Integer numberOfPlans, RoutingPlan selectedPlan, double ratioPre, double ratioPost, Enum strategy ) {
        InformationPage page = new InformationPage( "routing" + statement.getIndex(), "Routing", null )
                .setStmtLabel( statement.getIndex() )
                .fullWidth();
        InformationGroup overview = new InformationGroup( page, "Overview" ).setOrder( 1 );
        InformationTable overviewTable = new InformationTable( overview, ImmutableList.of( "Query Class", selectedPlan.getQueryClass() ) );
        overviewTable.addRow( "# of Proposed Plans", numberOfPlans == 0 ? "-" : numberOfPlans );
        overviewTable.addRow( "Pre Cost Factor", ratioPre );
        overviewTable.addRow( "Post Cost Factor", ratioPost );
        overviewTable.addRow( "Selection Strategy", strategy );
        if ( selectedPlan.getRoutedDistribution() != null ) {
            overviewTable.addRow( "Selected Plan", selectedPlan.getRoutedDistribution().toString() );
        }
        if ( selectedPlan.getRouter() != null ) {
            overviewTable.addRow( "Proposed By", selectedPlan.getRouter().getSimpleName() );
        }

        manager.addPage( page );
        manager.addGroup( overview );
        manager.registerInformation( overviewTable );

        return page;
    }


    private void registerSelectedAdapterTable( @Nullable RoutedDistribution distribution, InformationPage page ) {
        InformationGroup group = new InformationGroup( page, "Selected Placements" );
        InformationTable table = new InformationTable(
                group,
                ImmutableList.of( "Entity", "Placement Id", "Adapter", "Allocation Id" ) );
        if ( distribution != null ) {
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
        manager.addGroup( group );
        manager.registerInformation( table );
    }


    private void registerProposedPlans(
            List<AlgOptCost> approximatedCosts,
            List<Double> preCosts,
            List<Double> postCosts,
            List<Double> icarusCosts,
            List<? extends RoutingPlan> routingPlans,
            List<Double> effectiveCosts,
            List<Double> percentageCosts,
            InformationPage page ) {
        final boolean isIcarus = icarusCosts != null;
        InformationGroup group = new InformationGroup( page, "Proposed Plans" ).setOrder( 2 );
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
        manager.addGroup( group );
        manager.registerInformation( proposedPlansTable );
    }


    private InformationPage initPage( Statement statement, String title, String description ) {
        InformationPage page = new InformationPage( title, description );
        page.setStmtLabel( statement.getIndex() );
        manager.addPage( page );
        return page;
    }


    private InformationDuration initDuration( InformationPage page, String title, int order ) {
        InformationGroup group = new InformationGroup( page, title );
        group.setOrder( order );
        InformationDuration duration = new InformationDuration( group );
        manager.addGroup( group );
        manager.registerInformation( duration );
        return duration;
    }


    private void initTransactionsPage() {
        if ( transactionsPage == null ) {
            transactionsPage = new InformationPage( "Transactions", "Analysis of all transactions." )
                    .fullWidth();
            manager.addPage( transactionsPage );
        }
    }


    public static class TransactionAnalyzer {

        @Getter
        private final QueryAnalyzer analyzer;
        private final Transaction transaction;


        public TransactionAnalyzer( QueryAnalyzer analyzer, Transaction transaction ) {
            this.analyzer = analyzer;
            this.transaction = transaction;
        }


        public void registerFinished( String commitStatus ) {
            analyzer.registerTransaction( transaction, commitStatus );
        }


        public StatementAnalyzer createStatementAnalyzer( Statement statement ) {
            return new StatementAnalyzer( analyzer, statement );
        }

    }


    public static class StatementAnalyzer {

        @Getter
        private final QueryAnalyzer analyzer;
        private final Statement statement;

        private InformationPage executionTimePage;
        private final Map<String, InformationDuration> durations = new HashMap<>();

        private InformationPage routingPage;


        public StatementAnalyzer( QueryAnalyzer analyzer, Statement statement ) {
            this.analyzer = analyzer;
            this.statement = statement;
        }


        public void registerImplementation( String code ) {
            analyzer.registerImplementation( statement, code );
        }


        public void registerGeneratedCode( String code ) {
            analyzer.registerGeneratedCode( statement, code );
        }


        public void registerQueryPlan( PlanType planType, String jsonPolyAlg, @Nullable String textualPolyAlg ) {
            analyzer.registerQueryPlan( statement, planType, jsonPolyAlg, textualPolyAlg );
        }


        public void registerConstraintPlan( String jsonPolyAlg ) {
            analyzer.registerConstraintPlan( statement, jsonPolyAlg );
        }


        public void registerException( Throwable exception ) {
            analyzer.registerException( statement, exception );
        }


        public void registerRoutingBaseOutput( Integer numberOfPlans, RoutingPlan selectedPlan, double ratioPre, double ratioPost, Enum strategy ) {
            if ( routingPage == null ) {
                routingPage = analyzer.registerRoutingBaseOutput( statement, numberOfPlans, selectedPlan, ratioPre, ratioPost, strategy );
            }
        }


        public void registerSelectedAdapterTable( @Nullable RoutedDistribution distribution ) {
            analyzer.registerSelectedAdapterTable( distribution, routingPage );
        }


        public void registerProposedPlans(
                List<AlgOptCost> approximatedCosts,
                List<Double> preCosts,
                List<Double> postCosts,
                List<Double> icarusCosts,
                List<? extends RoutingPlan> routingPlans,
                List<Double> effectiveCosts,
                List<Double> percentageCosts ) {
            analyzer.registerProposedPlans( approximatedCosts, preCosts, postCosts, icarusCosts, routingPlans, effectiveCosts, percentageCosts, routingPage );
        }


        public InformationDuration getDuration( String title, int order ) {
            if ( executionTimePage == null ) {
                executionTimePage = analyzer.initPage( statement, "Execution Time", "Query processing & execution time" );
            }
            return durations.computeIfAbsent( title, k -> analyzer.initDuration( executionTimePage, k, order ) );
        }

    }

}
