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

package org.polypheny.db.processing;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataContext.ParameterValue;
import org.polypheny.db.adapter.enumerable.EnumerableAlg;
import org.polypheny.db.adapter.enumerable.EnumerableAlg.Prefer;
import org.polypheny.db.adapter.enumerable.EnumerableCalc;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableInterpretable;
import org.polypheny.db.adapter.index.Index;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.AlgStructuredTypeFlattener;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.common.BatchIterator;
import org.polypheny.db.algebra.core.common.ConditionalExecute;
import org.polypheny.db.algebra.core.common.ConditionalExecute.Condition;
import org.polypheny.db.algebra.core.common.ConstraintEnforcer;
import org.polypheny.db.algebra.core.document.DocumentAlg;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationCode;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.interpreter.Interpreters;
import org.polypheny.db.monitoring.events.DmlEvent;
import org.polypheny.db.monitoring.events.QueryEvent;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.prepare.Prepare.PreparedResult;
import org.polypheny.db.prepare.Prepare.PreparedResultImpl;
import org.polypheny.db.processing.caching.ImplementationCache;
import org.polypheny.db.processing.caching.QueryPlanCache;
import org.polypheny.db.processing.caching.RoutingPlanCache;
import org.polypheny.db.processing.shuttles.LogicalQueryInformationImpl;
import org.polypheny.db.processing.shuttles.ParameterValueValidator;
import org.polypheny.db.processing.shuttles.QueryParameterizer;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.routing.DmlRouter;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.routing.ExecutionTimeMonitor.ExecutionTimeObserver;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.ProposedRoutingPlan;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.routing.RoutingPlan;
import org.polypheny.db.routing.UiRoutingPageUtil;
import org.polypheny.db.routing.dto.CachedProposedRoutingPlan;
import org.polypheny.db.routing.dto.ProposedRoutingPlanImpl;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.Typed;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.ModelTraitDef;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.Program;
import org.polypheny.db.tools.Programs;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.EntityAccessMap;
import org.polypheny.db.transaction.EntityAccessMap.EntityIdentifier;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;
import org.polypheny.db.view.MaterializedViewManager;
import org.polypheny.db.view.MaterializedViewManager.TableUpdateVisitor;
import org.polypheny.db.view.ViewManager.ViewVisitor;


@Slf4j
public abstract class AbstractQueryProcessor implements QueryProcessor, ExecutionTimeObserver {

    protected static final boolean ENABLE_BINDABLE = false;
    protected static final boolean ENABLE_COLLATION_TRAIT = true;
    protected static final boolean ENABLE_ENUMERABLE = true;
    protected static final boolean ENABLE_MODEL_TRAIT = true;
    protected static final boolean CONSTANT_REDUCTION = false;
    protected static final boolean ENABLE_STREAM = true;
    private final Statement statement;

    // This map is required to allow plans with multiple physical placements of the same logical table.
    // scanId -> tableId
    private final Map<Integer, Long> scanPerTable = new HashMap<>();


    protected AbstractQueryProcessor( Statement statement ) {
        this.statement = statement;
    }


    @Override
    public void executionTime( String reference, long nanoTime ) {
        StatementEvent event = statement.getMonitoringEvent();
        if ( reference.equals( event.getLogicalQueryInformation().getQueryClass() ) ) {
            event.setExecutionTime( nanoTime );
        }
    }


    @Override
    public void resetCaches() {
        ImplementationCache.INSTANCE.reset();
        QueryPlanCache.INSTANCE.reset();
        RoutingPlanCache.INSTANCE.reset();
        RoutingManager.getInstance().getRouters().forEach( Router::resetCaches );
    }


    @Override
    public PolyImplementation prepareQuery( AlgRoot logicalRoot, boolean withMonitoring ) {
        return prepareQuery( logicalRoot, logicalRoot.alg.getCluster().getTypeFactory().builder().build(), false, false, withMonitoring );
    }


    @Override
    public PolyImplementation prepareQuery( AlgRoot logicalRoot, AlgDataType parameterRowType, boolean withMonitoring ) {
        return prepareQuery( logicalRoot, parameterRowType, false, false, withMonitoring );
    }


    @Override
    public PolyImplementation prepareQuery( AlgRoot logicalRoot, AlgDataType parameterRowType, boolean isRouted, boolean isSubquery, boolean withMonitoring ) {

        if ( statement.getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "Logical Query Plan" ).setLabel( "plans" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Logical Query Plan" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    group,
                    AlgOptUtil.dumpPlan( "Logical Query Plan", logicalRoot.alg, ExplainFormat.JSON, ExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        }

        if ( statement.getTransaction().isAnalyze() ) {
            statement.getOverviewDuration().start( "Processing" );
        }
        final ProposedImplementations proposedImplementations = prepareQueryList( logicalRoot, parameterRowType, isRouted, isSubquery );

        if ( statement.getTransaction().isAnalyze() ) {
            statement.getOverviewDuration().stop( "Processing" );
            statement.getOverviewDuration().start( "Plan Selection" );
        }

        final Pair<PolyImplementation, ProposedRoutingPlan> selectedPlan = selectPlan( proposedImplementations );

        if ( statement.getTransaction().isAnalyze() ) {
            statement.getOverviewDuration().stop( "Plan Selection" );
        }

        if ( withMonitoring ) {
            this.monitorResult( selectedPlan.right );
        }

        return selectedPlan.left;
    }


    private ProposedImplementations prepareQueryList( AlgRoot logicalRoot, AlgDataType parameterRowType, boolean isRouted, boolean isSubQuery ) {
        boolean isAnalyze = statement.getTransaction().isAnalyze() && !isSubQuery;
        boolean lock = !isSubQuery;

        final Convention resultConvention = ENABLE_BINDABLE ? BindableConvention.INSTANCE : EnumerableConvention.INSTANCE;
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        // Initialize result lists. They will all be with in the same ordering.
        List<ProposedRoutingPlan> proposedRoutingPlans = null;
        List<AlgNode> optimalNodeList = new ArrayList<>();
        List<AlgRoot> parameterizedRootList = new ArrayList<>();
        List<PolyImplementation> results = new ArrayList<>();
        List<String> generatedCodes = new ArrayList<>();

        //
        // Check for view
        if ( logicalRoot.alg.hasView() ) {
            logicalRoot = logicalRoot.tryExpandView();
        }

        // Analyze step
        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Analyze" );
        }

        // Analyze query, get logical partitions, queryId and initialize monitoring
        LogicalQueryInformation logicalQueryInformation = this.analyzeQueryAndPrepareMonitoring( statement, logicalRoot, isAnalyze, isSubQuery );

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Analyze" );
        }

        ExecutionTimeMonitor executionTimeMonitor = new ExecutionTimeMonitor();
        if ( RoutingManager.POST_COST_AGGREGATION_ACTIVE.getBoolean() ) {
            // Subscribe only when aggregation is active
            executionTimeMonitor.subscribe( this, logicalQueryInformation.getQueryClass() );
        }

        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Expand Views" );
        }

        // Check if the relRoot includes Views or Materialized Views and replaces what necessary
        // View: replace LogicalViewScan with underlying information
        // Materialized View: add order by if Materialized View includes Order by
        ViewVisitor viewVisitor = new ViewVisitor( false );
        logicalRoot = viewVisitor.startSubstitution( logicalRoot );

        // Update which tables where changed used for Materialized Views
        TableUpdateVisitor visitor = new TableUpdateVisitor();
        logicalRoot.alg.accept( visitor );
        MaterializedViewManager.getInstance().addTables( statement.getTransaction(), visitor.getNames() );

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Expand Views" );
            statement.getProcessingDuration().start( "Parameter Validation" );
        }

        //
        // Validate parameter values
        ParameterValueValidator valueValidator = new ParameterValueValidator( logicalRoot.validatedRowType, statement.getDataContext() );
        valueValidator.visit( logicalRoot.alg );

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Parameter Validation" );
        }

        if ( isRouted ) {
            proposedRoutingPlans = Lists.newArrayList( new ProposedRoutingPlanImpl( logicalRoot, logicalQueryInformation.getQueryClass() ) );
        } else {
            //
            // Locking
            if ( isAnalyze ) {
                statement.getProcessingDuration().start( "Locking" );
            }
            if ( lock ) {
                this.acquireLock( isAnalyze, logicalRoot, logicalQueryInformation.getAccessedPartitions() );
            }

            //
            // Index Update
            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Locking" );
                statement.getProcessingDuration().start( "Index Update" );
            }
            AlgRoot indexUpdateRoot = logicalRoot;
            if ( RuntimeConfig.POLYSTORE_INDEXES_ENABLED.getBoolean() ) {
                IndexManager.getInstance().barrier( statement.getTransaction().getXid() );
                indexUpdateRoot = indexUpdate( indexUpdateRoot, statement, parameterRowType );
            }

            //
            // Constraint Enforcement Rewrite
            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Index Update" );
                statement.getProcessingDuration().start( "Constraint Enforcement" );
            }
            AlgRoot constraintsRoot = indexUpdateRoot;

            if ( constraintsRoot.kind.belongsTo( Kind.DML ) && (RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean() || RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean()) ) {
                constraintsRoot = ConstraintEnforceAttacher.handleConstraints( constraintsRoot, statement );
            }

            //
            // Index Lookup Rewrite
            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Constraint Enforcement" );
                statement.getProcessingDuration().start( "Index Lookup Rewrite" );
            }

            AlgRoot indexLookupRoot = constraintsRoot;
            if ( RuntimeConfig.POLYSTORE_INDEXES_ENABLED.getBoolean() && RuntimeConfig.POLYSTORE_INDEXES_SIMPLIFY.getBoolean() ) {
                indexLookupRoot = indexLookup( indexLookupRoot, statement );
            }
            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Index Lookup Rewrite" );
                statement.getProcessingDuration().start( "Routing" );
            }

            //
            // Routing
            if ( RuntimeConfig.ROUTING_PLAN_CACHING.getBoolean() && !indexLookupRoot.kind.belongsTo( Kind.DML ) ) {
                Set<Long> partitionIds = logicalQueryInformation.getAccessedPartitions().values().stream()
                        .flatMap( List::stream )
                        .collect( Collectors.toSet() );
                List<CachedProposedRoutingPlan> routingPlansCached = RoutingPlanCache.INSTANCE.getIfPresent( logicalQueryInformation.getQueryClass(), partitionIds );
                if ( !routingPlansCached.isEmpty() && routingPlansCached.stream().noneMatch( p -> p.physicalPlacementsOfPartitions.isEmpty() ) ) {
                    proposedRoutingPlans = routeCached( indexLookupRoot, routingPlansCached, statement, logicalQueryInformation, isAnalyze );
                }
            }

            if ( proposedRoutingPlans == null ) {
                proposedRoutingPlans = route( indexLookupRoot, statement, logicalQueryInformation );
            }

            if ( isAnalyze ) {
                statement.getRoutingDuration().start( "Flattener" );
            }

            proposedRoutingPlans.forEach( proposedRoutingPlan -> {
                AlgRoot routedRoot = proposedRoutingPlan.getRoutedRoot();
                AlgStructuredTypeFlattener typeFlattener = new AlgStructuredTypeFlattener(
                        AlgBuilder.create( statement, routedRoot.alg.getCluster() ),
                        routedRoot.alg.getCluster().getRexBuilder(),
                        routedRoot.alg::getCluster,
                        true );
                proposedRoutingPlan.setRoutedRoot( routedRoot.withAlg( typeFlattener.rewrite( routedRoot.alg ) ) );
            } );

            if ( isAnalyze ) {
                statement.getRoutingDuration().stop( "Flattener" );
                statement.getProcessingDuration().stop( "Routing" );
            }
        }

        //
        // Parameterize
        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Parameterize" );
        }

        // Add optional parameterizedRoots and results for all routed RelRoots.
        // Index of routedRoot, parameterizedRootList and results correspond!
        for ( ProposedRoutingPlan routingPlan : proposedRoutingPlans ) {
            AlgRoot routedRoot = routingPlan.getRoutedRoot();
            AlgRoot parameterizedRoot;
            if ( statement.getDataContext().getParameterValues().size() == 0
                    && (RuntimeConfig.PARAMETERIZE_DML.getBoolean() || !routedRoot.kind.belongsTo( Kind.DML )) ) {
                Pair<AlgRoot, AlgDataType> parameterized = parameterize( routedRoot, parameterRowType );
                parameterizedRoot = parameterized.left;
            } else {
                // This query is an execution of a prepared statement
                parameterizedRoot = routedRoot;
            }

            parameterizedRootList.add( parameterizedRoot );
        }

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Parameterize" );
        }

        //
        // Implementation Caching
        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Implementation Caching" );
        }

        for ( int i = 0; i < proposedRoutingPlans.size(); i++ ) {
            AlgRoot routedRoot = proposedRoutingPlans.get( i ).getRoutedRoot();
            if ( this.isImplementationCachingActive( statement, routedRoot ) ) {
                AlgRoot parameterizedRoot = parameterizedRootList.get( i );
                PreparedResult preparedResult = ImplementationCache.INSTANCE.getIfPresent( parameterizedRoot.alg );
                AlgNode optimalNode = QueryPlanCache.INSTANCE.getIfPresent( parameterizedRootList.get( i ).alg );
                if ( preparedResult != null ) {
                    PolyImplementation result = createPolyImplementation(
                            preparedResult,
                            parameterizedRoot.kind,
                            optimalNode,
                            parameterizedRoot.validatedRowType,
                            resultConvention,
                            executionTimeMonitor,
                            Objects.requireNonNull( optimalNode.getTraitSet().getTrait( ModelTraitDef.INSTANCE ) ).getDataModel() );
                    results.add( result );
                    generatedCodes.add( preparedResult.getCode() );
                    optimalNodeList.add( optimalNode );
                } else {
                    results.add( null );
                    generatedCodes.add( null );
                    optimalNodeList.add( null );
                }
            } else {
                results.add( null );
                generatedCodes.add( null );
                optimalNodeList.add( null );
            }
        }

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Implementation Caching" );
        }

        // Can we return earlier?
        if ( results.stream().allMatch( Objects::nonNull ) && optimalNodeList.stream().allMatch( Objects::nonNull ) ) {
            return new ProposedImplementations(
                    proposedRoutingPlans,
                    optimalNodeList.stream().filter( Objects::nonNull ).collect( Collectors.toList() ),
                    results.stream().filter( Objects::nonNull ).collect( Collectors.toList() ),
                    generatedCodes.stream().filter( Objects::nonNull ).collect( Collectors.toList() ),
                    logicalQueryInformation );
        }

        optimalNodeList = new ArrayList<>( Collections.nCopies( optimalNodeList.size(), null ) );

        //
        // Plan Caching
        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Plan Caching" );
        }
        for ( int i = 0; i < proposedRoutingPlans.size(); i++ ) {
            if ( this.isQueryPlanCachingActive( statement, proposedRoutingPlans.get( i ).getRoutedRoot() ) ) {
                // Should always be the case
                AlgNode cachedElem = QueryPlanCache.INSTANCE.getIfPresent( parameterizedRootList.get( i ).alg );
                if ( cachedElem != null ) {
                    optimalNodeList.set( i, cachedElem );
                }
            }
        }

        //
        // Planning & Optimization
        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Plan Caching" );
            statement.getProcessingDuration().start( "Planning & Optimization" );
        }

        // OptimalNode same size as routed, parametrized and result
        for ( int i = 0; i < optimalNodeList.size(); i++ ) {
            if ( optimalNodeList.get( i ) != null ) {
                continue;
            }
            AlgRoot parameterizedRoot = parameterizedRootList.get( i );
            AlgRoot routedRoot = proposedRoutingPlans.get( i ).getRoutedRoot();
            optimalNodeList.set( i, optimize( parameterizedRoot, resultConvention ) );

            if ( this.isQueryPlanCachingActive( statement, routedRoot ) ) {
                QueryPlanCache.INSTANCE.put( parameterizedRoot.alg, optimalNodeList.get( i ) );
            }
        }

        //
        // Implementation
        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Planning & Optimization" );
            statement.getProcessingDuration().start( "Implementation" );
        }

        for ( int i = 0; i < optimalNodeList.size(); i++ ) {
            if ( results.get( i ) != null ) {
                continue;
            }

            AlgNode optimalNode = optimalNodeList.get( i );
            AlgRoot parameterizedRoot = parameterizedRootList.get( i );
            AlgRoot routedRoot = proposedRoutingPlans.get( i ).getRoutedRoot();

            final AlgDataType rowType = parameterizedRoot.alg.getRowType();
            final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
            AlgRoot optimalRoot = new AlgRoot( optimalNode, rowType, parameterizedRoot.kind, fields, algCollation( parameterizedRoot.alg ) );

            PreparedResult preparedResult = implement( optimalRoot, parameterRowType );

            // Cache implementation
            if ( this.isImplementationCachingActive( statement, routedRoot ) ) {
                if ( optimalRoot.alg.isImplementationCacheable() ) {
                    ImplementationCache.INSTANCE.put( parameterizedRoot.alg, preparedResult );
                } else {
                    ImplementationCache.INSTANCE.countUncacheable();
                }
            }

            PolyImplementation result = createPolyImplementation(
                    preparedResult,
                    optimalRoot.kind,
                    optimalRoot.alg,
                    optimalRoot.validatedRowType,
                    resultConvention,
                    executionTimeMonitor,
                    Objects.requireNonNull( optimalNode.getTraitSet().getTrait( ModelTraitDef.INSTANCE ) ).getDataModel() );
            results.set( i, result );
            generatedCodes.set( i, preparedResult.getCode() );
            optimalNodeList.set( i, optimalRoot.alg );
        }
        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Implementation" );
        }

        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Preparing statement ... done. [{}]", stopWatch );
        }

        // Finally, all optionals should be of certain values.
        return new ProposedImplementations(
                proposedRoutingPlans,
                optimalNodeList.stream().filter( Objects::nonNull ).collect( Collectors.toList() ),
                results.stream().filter( Objects::nonNull ).collect( Collectors.toList() ),
                generatedCodes.stream().filter( Objects::nonNull ).collect( Collectors.toList() ),
                logicalQueryInformation );
    }


    @AllArgsConstructor
    @Getter
    private static class ProposedImplementations {

        private final List<ProposedRoutingPlan> proposedRoutingPlans;
        private final List<AlgNode> optimizedPlans;
        private final List<PolyImplementation> results;
        private final List<String> generatedCodes;
        private final LogicalQueryInformation logicalQueryInformation;

    }


    private void acquireLock( boolean isAnalyze, AlgRoot logicalRoot, Map<Integer, List<Long>> accessedPartitions ) {
        // TODO @HENNLO Check if this is this is necessary to pass the partitions explicitly.
        // This currently only works for queries. Since DMLs are evaluated during routing.
        // This SHOULD be adjusted

        // Locking
        try {
            Collection<Entry<EntityIdentifier, LockMode>> idAccessMap = new ArrayList<>();
            // Get locks for individual entities
            EntityAccessMap accessMap = new EntityAccessMap( logicalRoot.alg, accessedPartitions );
            // Get a shared global schema lock (only DDLs acquire an exclusive global schema lock)
            idAccessMap.add( Pair.of( LockManager.GLOBAL_LOCK, LockMode.SHARED ) );

            idAccessMap.addAll( accessMap.getAccessedEntityPair() );
            LockManager.INSTANCE.lock( idAccessMap, (TransactionImpl) statement.getTransaction() );
        } catch ( DeadlockException e ) {
            throw new RuntimeException( e );
        }
    }


    private AlgRoot indexUpdate( AlgRoot root, Statement statement, AlgDataType parameterRowType ) {
        if ( root.kind.belongsTo( Kind.DML ) ) {
            final AlgShuttle shuttle = new AlgShuttleImpl() {

                @Override
                public AlgNode visit( AlgNode node ) {
                    RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
                    if ( node instanceof LogicalModify ) {
                        final Catalog catalog = Catalog.getInstance();
                        final LogicalModify ltm = (LogicalModify) node;
                        final CatalogTable table;
                        final CatalogSchema schema;
                        try {
                            String tableName;
                            if ( ltm.getTable().getQualifiedName().size() == 3 ) { // DatabaseName.SchemaName.TableName
                                schema = catalog.getSchema( ltm.getTable().getQualifiedName().get( 0 ), ltm.getTable().getQualifiedName().get( 1 ) );
                                tableName = ltm.getTable().getQualifiedName().get( 2 );
                            } else if ( ltm.getTable().getQualifiedName().size() == 2 ) { // SchemaName.TableName
                                schema = catalog.getSchema( statement.getPrepareContext().getDatabaseId(), ltm.getTable().getQualifiedName().get( 0 ) );
                                tableName = ltm.getTable().getQualifiedName().get( 1 );
                            } else { // TableName
                                schema = catalog.getSchema( statement.getPrepareContext().getDatabaseId(), statement.getPrepareContext().getDefaultSchemaName() );
                                tableName = ltm.getTable().getQualifiedName().get( 0 );
                            }
                            table = catalog.getTable( schema.id, tableName );
                        } catch ( UnknownTableException | UnknownDatabaseException | UnknownSchemaException e ) {
                            // This really should not happen
                            log.error( "Table not found: {}", ltm.getTable().getQualifiedName().get( 0 ), e );
                            throw new RuntimeException( e );
                        }
                        final List<Index> indices = IndexManager.getInstance().getIndices( schema, table );

                        // Check if there are any indexes effected by this table modify
                        if ( indices.size() == 0 ) {
                            // Nothing to do here
                            return super.visit( node );
                        }

                        if ( ltm.isInsert() && ltm.getInput() instanceof Values ) {
                            final LogicalValues lvalues = (LogicalValues) ltm.getInput( 0 ).accept( new DeepCopyShuttle() );
                            for ( final Index index : indices ) {
                                final Set<Pair<List<Object>, List<Object>>> tuplesToInsert = new HashSet<>( lvalues.tuples.size() );
                                for ( final ImmutableList<RexLiteral> row : lvalues.getTuples() ) {
                                    final List<Object> rowValues = new ArrayList<>();
                                    final List<Object> targetRowValues = new ArrayList<>();
                                    for ( final String column : index.getColumns() ) {
                                        final RexLiteral fieldValue = row.get(
                                                lvalues.getRowType().getField( column, false, false ).getIndex()
                                        );
                                        rowValues.add( fieldValue.getValue2() );
                                    }
                                    for ( final String column : index.getTargetColumns() ) {
                                        final RexLiteral fieldValue = row.get(
                                                lvalues.getRowType().getField( column, false, false ).getIndex()
                                        );
                                        targetRowValues.add( fieldValue.getValue2() );
                                    }
                                    tuplesToInsert.add( new Pair<>( rowValues, targetRowValues ) );
                                }
                                index.insertAll( statement.getTransaction().getXid(), tuplesToInsert );
                            }
                        } else if ( ltm.isInsert() && ltm.getInput() instanceof LogicalProject && ((LogicalProject) ltm.getInput()).getInput().getRowType().toString().equals( "RecordType(INTEGER ZERO)" ) ) {
                            final LogicalProject lproject = (LogicalProject) ltm.getInput().accept( new DeepCopyShuttle() );
                            for ( final Index index : indices ) {
                                final Set<Pair<List<Object>, List<Object>>> tuplesToInsert = new HashSet<>( lproject.getProjects().size() );
                                final List<Object> rowValues = new ArrayList<>();
                                final List<Object> targetRowValues = new ArrayList<>();
                                for ( final String column : index.getColumns() ) {
                                    final RexNode fieldValue = lproject.getProjects().get(
                                            lproject.getRowType().getField( column, false, false ).getIndex()
                                    );
                                    if ( fieldValue instanceof RexLiteral ) {
                                        rowValues.add( ((RexLiteral) fieldValue).getValue2() );
                                    } else if ( fieldValue instanceof RexDynamicParam ) {
                                        //
                                        // TODO: This is dynamic parameter. We need to do the index update in the generated code!
                                        //
                                        throw new RuntimeException( "Index updates are not yet supported for prepared statements" );
                                    } else {
                                        throw new RuntimeException( "Unexpected rex type: " + fieldValue.getClass() );
                                    }
                                }
                                for ( final String column : index.getTargetColumns() ) {
                                    final RexNode fieldValue = lproject.getProjects().get(
                                            lproject.getRowType().getField( column, false, false ).getIndex()
                                    );
                                    if ( fieldValue instanceof RexLiteral ) {
                                        targetRowValues.add( ((RexLiteral) fieldValue).getValue2() );
                                    } else if ( fieldValue instanceof RexDynamicParam ) {
                                        //
                                        // TODO: This is dynamic parameter. We need to do the index update in the generated code!
                                        //
                                        throw new RuntimeException( "Index updates are not yet supported for prepared statements" );
                                    } else {
                                        throw new RuntimeException( "Unexpected rex type: " + fieldValue.getClass() );
                                    }
                                }
                                tuplesToInsert.add( new Pair<>( rowValues, targetRowValues ) );
                                index.insertAll( statement.getTransaction().getXid(), tuplesToInsert );
                            }
                        } else if ( ltm.isDelete() || ltm.isUpdate() || ltm.isMerge() || (ltm.isInsert() && !(ltm.getInput() instanceof Values)) ) {
                            final Map<String, Integer> nameMap = new HashMap<>();
                            final Map<String, Integer> newValueMap = new HashMap<>();
                            AlgNode original = ltm.getInput().accept( new DeepCopyShuttle() );
                            if ( !(original instanceof LogicalProject) ) {
                                original = LogicalProject.identity( original );
                            }
                            LogicalProject originalProject = (LogicalProject) original;

                            for ( int i = 0; i < originalProject.getNamedProjects().size(); ++i ) {
                                final Pair<RexNode, String> np = originalProject.getNamedProjects().get( i );
                                nameMap.put( np.right, i );
                                if ( ltm.isUpdate() || ltm.isMerge() ) {
                                    int j = ltm.getUpdateColumnList().indexOf( np.right );
                                    if ( j >= 0 ) {
                                        RexNode newValue = ltm.getSourceExpressionList().get( j );
                                        for ( int k = 0; k < originalProject.getNamedProjects().size(); ++k ) {
                                            if ( originalProject.getNamedProjects().get( k ).left.equals( newValue ) ) {
                                                newValueMap.put( np.right, k );
                                                break;
                                            }
                                        }
                                    }
                                } else if ( ltm.isInsert() ) {
                                    int j = originalProject.getRowType().getField( np.right, true, false ).getIndex();
                                    if ( j >= 0 ) {
                                        RexNode newValue = rexBuilder.makeInputRef( originalProject, j );
                                        for ( int k = 0; k < originalProject.getNamedProjects().size(); ++k ) {
                                            if ( originalProject.getNamedProjects().get( k ).left.equals( newValue ) ) {
                                                newValueMap.put( np.right, k );
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            // Prepare subquery
//                            if ( ltm.isUpdate() || ltm.isMerge() ) {
//                                List<RexNode> expr = new ArrayList<>(  );
//                                //FIXME(s3lph) some index out of bounds stuff
//                                for ( final String name : originalProject.getRowType().getFieldNames() ) {
//                                    if ( ltm.getUpdateColumnList().contains( name )) {
//                                        expr.add( ltm.getSourceExpressionList().get( ltm.getUpdateColumnList().indexOf( name ) ) );
//                                    } else {
//                                        expr.add( rexBuilder.makeInputRef( originalProject, originalProject.getRowType().getField( name, true, false ).getIndex() ) );
//                                    }
//                                }
//                                List<RelDataType> types = ltm.getSourceExpressionList().stream().map( RexNode::getType ).collect( Collectors.toList() );
//                                RelDataType type = transaction.getTypeFactory().createStructType( types, originalProject.getRowType().getFieldNames() );
//                                originalProject = LogicalProject.create( originalProject, expr, type );
//                            }
                            AlgRoot scanRoot = AlgRoot.of( originalProject, Kind.SELECT );
                            final PolyImplementation scanSig = prepareQuery( scanRoot, parameterRowType, false, false, true );
                            final List<List<Object>> rows = scanSig.getRows( statement, -1 );
                            // Build new query tree
                            final List<ImmutableList<RexLiteral>> records = new ArrayList<>();
                            for ( final List<Object> row : rows ) {
                                final List<RexLiteral> record = new ArrayList<>();
                                for ( int i = 0; i < row.size(); ++i ) {
                                    AlgDataType fieldType = originalProject.getRowType().getFieldList().get( i ).getType();
                                    Pair<Comparable, PolyType> converted = RexLiteral.convertType( (Comparable) row.get( i ), fieldType );
                                    record.add( new RexLiteral(
                                            converted.left,
                                            fieldType,
                                            converted.right
                                    ) );
                                }
                                records.add( ImmutableList.copyOf( record ) );
                            }
                            final ImmutableList<ImmutableList<RexLiteral>> values = ImmutableList.copyOf( records );
                            final AlgNode newValues = LogicalValues.create( originalProject.getCluster(), originalProject.getRowType(), values );
                            final AlgNode newProject = LogicalProject.identity( newValues );
//                            List<RexNode> sourceExpr = ltm.getSourceExpressionList();
//                            if ( ltm.isUpdate() || ltm.isMerge() ) {
//                                //FIXME(s3lph): Wrong index
//                                sourceExpr = IntStream.range( 0, sourceExpr.size() )
//                                        .mapToObj( i -> rexBuilder.makeFieldAccess( rexBuilder.makeInputRef( newProject, i ), 0 ) )
//                                        .collect( Collectors.toList() );
//                            }
//                            final {@link AlgNode} replacement = LogicalModify.create(
//                                    ltm.getTable(),
//                                    transaction.getCatalogReader(),
//                                    newProject,
//                                    ltm.getOperation(),
//                                    ltm.getUpdateColumnList(),
//                                    sourceExpr,
//                                    ltm.isFlattened()
//                            );
                            final AlgNode replacement = ltm.copy( ltm.getTraitSet(), Collections.singletonList( newProject ) );
                            // Schedule the index deletions
                            if ( !ltm.isInsert() ) {
                                for ( final Index index : indices ) {
                                    if ( ltm.isUpdate() ) {
                                        // Index not affected by this update, skip
                                        if ( index.getColumns().stream().noneMatch( ltm.getUpdateColumnList()::contains ) ) {
                                            continue;
                                        }
                                    }
                                    final Set<Pair<List<Object>, List<Object>>> rowsToDelete = new HashSet<>( rows.size() );
                                    for ( List<Object> row : rows ) {
                                        final List<Object> rowProjection = new ArrayList<>( index.getColumns().size() );
                                        final List<Object> targetRowProjection = new ArrayList<>( index.getTargetColumns().size() );
                                        for ( final String column : index.getColumns() ) {
                                            rowProjection.add( row.get( nameMap.get( column ) ) );
                                        }
                                        for ( final String column : index.getTargetColumns() ) {
                                            targetRowProjection.add( row.get( nameMap.get( column ) ) );
                                        }
                                        rowsToDelete.add( new Pair<>( rowProjection, targetRowProjection ) );
                                    }
                                    index.deleteAllPrimary( statement.getTransaction().getXid(), rowsToDelete );
                                }
                            }
                            //Schedule the index insertions for INSERT and UPDATE operations
                            if ( !ltm.isDelete() ) {
                                for ( final Index index : indices ) {
                                    // Index not affected by this update, skip
                                    if ( ltm.isUpdate() && index.getColumns().stream().noneMatch( ltm.getUpdateColumnList()::contains ) ) {
                                        continue;
                                    }
                                    if ( ltm.isInsert() && index.getColumns().stream().noneMatch( ltm.getInput().getRowType().getFieldNames()::contains ) ) {
                                        continue;
                                    }
                                    final Set<Pair<List<Object>, List<Object>>> rowsToReinsert = new HashSet<>( rows.size() );
                                    for ( List<Object> row : rows ) {
                                        final List<Object> rowProjection = new ArrayList<>( index.getColumns().size() );
                                        final List<Object> targetRowProjection = new ArrayList<>( index.getTargetColumns().size() );
                                        for ( final String column : index.getColumns() ) {
                                            if ( newValueMap.containsKey( column ) ) {
                                                rowProjection.add( row.get( newValueMap.get( column ) ) );
                                            } else {
                                                // Value unchanged, reuse old value
                                                rowProjection.add( row.get( nameMap.get( column ) ) );
                                            }
                                        }
                                        for ( final String column : index.getTargetColumns() ) {
                                            targetRowProjection.add( row.get( nameMap.get( column ) ) );
                                        }
                                        rowsToReinsert.add( new Pair<>( rowProjection, targetRowProjection ) );
                                    }
                                    index.insertAll( statement.getTransaction().getXid(), rowsToReinsert );
                                }
                            }
                            return replacement;
                        }

                    }
                    return super.visit( node );
                }

            };
            final AlgNode newRoot = shuttle.visit( root.alg );
            return AlgRoot.of( newRoot, root.kind );

        }
        return root;
    }


    private AlgRoot indexLookup( AlgRoot logicalRoot, Statement statement ) {
        final AlgBuilder builder = AlgBuilder.create( statement, logicalRoot.alg.getCluster() );
        final RexBuilder rexBuilder = builder.getRexBuilder();
        AlgNode newRoot = logicalRoot.alg;
        if ( logicalRoot.kind.belongsTo( Kind.DML ) ) {
            final AlgShuttle shuttle = new AlgShuttleImpl() {

                @Override
                public AlgNode visit( AlgNode node ) {
                    if ( node instanceof LogicalConditionalExecute ) {
                        final LogicalConditionalExecute lce = (LogicalConditionalExecute) node;
                        final Index index = IndexManager.getInstance().getIndex(
                                lce.getCatalogSchema(),
                                lce.getCatalogTable(),
                                lce.getCatalogColumns()
                        );
                        if ( index != null ) {
                            final LogicalConditionalExecute visited = (LogicalConditionalExecute) super.visit( lce );
                            Condition c = null;
                            switch ( lce.getCondition() ) {
                                case TRUE:
                                case FALSE:
                                    c = lce.getCondition();
                                    break;
                                case EQUAL_TO_ZERO:
                                    c = index.containsAny( statement.getTransaction().getXid(), lce.getValues() ) ? Condition.FALSE : Condition.TRUE;
                                    break;
                                case GREATER_ZERO:
                                    c = index.containsAny( statement.getTransaction().getXid(), lce.getValues() ) ? Condition.TRUE : Condition.FALSE;
                                    break;
                            }
                            final LogicalConditionalExecute simplified =
                                    LogicalConditionalExecute.create( visited.getLeft(), visited.getRight(), c, visited.getExceptionClass(), visited.getExceptionMessage() );
                            simplified.setCheckDescription( lce.getCheckDescription() );
                        }
                    }
                    return super.visit( node );
                }

            };
            newRoot = newRoot.accept( shuttle );
        }
        final AlgShuttle shuttle2 = new AlgShuttleImpl() {

            @Override
            public AlgNode visit( LogicalProject project ) {
                if ( project.getInput() instanceof LogicalScan ) {
                    // Figure out the original column names required for index lookup
                    final LogicalScan scan = (LogicalScan) project.getInput();
                    final String table = scan.getTable().getQualifiedName().get( scan.getTable().getQualifiedName().size() - 1 );
                    final List<String> columns = new ArrayList<>( project.getChildExps().size() );
                    final List<AlgDataType> ctypes = new ArrayList<>( project.getChildExps().size() );
                    for ( final RexNode expr : project.getChildExps() ) {
                        if ( !(expr instanceof RexInputRef) ) {
                            IndexManager.getInstance().incrementMiss();
                            return super.visit( project );
                        }
                        final RexInputRef rir = (RexInputRef) expr;
                        final AlgDataTypeField field = scan.getRowType().getFieldList().get( rir.getIndex() );
                        final String column = field.getName();
                        columns.add( column );
                        ctypes.add( field.getType() );
                    }
                    // Retrieve the catalog schema and database representations required for index lookup
                    final CatalogSchema schema = statement.getTransaction().getDefaultSchema();
                    final CatalogTable ctable;
                    try {
                        ctable = Catalog.getInstance().getTable( schema.id, table );
                    } catch ( UnknownTableException e ) {
                        log.error( "Could not fetch table", e );
                        IndexManager.getInstance().incrementNoIndex();
                        return super.visit( project );
                    }
                    // Retrieve any index and use for simplification
                    final Index idx = IndexManager.getInstance().getIndex( schema, ctable, columns );
                    if ( idx == null ) {
                        // No index available for simplification
                        IndexManager.getInstance().incrementNoIndex();
                        return super.visit( project );
                    }
                    // TODO: Avoid copying stuff around
                    final AlgDataType compositeType = builder.getTypeFactory().createStructType( ctypes, columns );
                    final Values replacement = idx.getAsValues( statement.getTransaction().getXid(), builder, compositeType );
                    final LogicalProject rProject = new LogicalProject(
                            replacement.getCluster(),
                            replacement.getTraitSet(),
                            replacement,
                            IntStream.range( 0, compositeType.getFieldCount() )
                                    .mapToObj( i -> rexBuilder.makeInputRef( replacement, i ) )
                                    .collect( Collectors.toList() ),
                            compositeType );
                    IndexManager.getInstance().incrementHit();
                    return rProject;
                }
                return super.visit( project );
            }


            @Override
            public AlgNode visit( AlgNode node ) {
                if ( node instanceof LogicalProject ) {
                    final LogicalProject lp = (LogicalProject) node;
                    lp.getMapping();
                }
                return super.visit( node );
            }

        };
        newRoot = newRoot.accept( shuttle2 );
        return AlgRoot.of( newRoot, logicalRoot.kind );
    }


    private List<ProposedRoutingPlan> route( AlgRoot logicalRoot, Statement statement, LogicalQueryInformation queryInformation ) {
        final DmlRouter dmlRouter = RoutingManager.getInstance().getDmlRouter();
        if ( logicalRoot.getModel() == ModelTrait.GRAPH ) {
            return routeGraph( logicalRoot, queryInformation, dmlRouter );
        } else if ( logicalRoot.getModel() == ModelTrait.DOCUMENT ) {
            return routeDocument( logicalRoot, queryInformation, dmlRouter );
        } else if ( logicalRoot.alg instanceof LogicalModify ) {
            AlgNode routedDml = dmlRouter.routeDml( (LogicalModify) logicalRoot.alg, statement );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedDml, logicalRoot, queryInformation.getQueryClass() ) );
        } else if ( logicalRoot.alg instanceof ConditionalExecute ) {
            AlgNode routedConditionalExecute = dmlRouter.handleConditionalExecute( logicalRoot.alg, statement, queryInformation );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedConditionalExecute, logicalRoot, queryInformation.getQueryClass() ) );
        } else if ( logicalRoot.alg instanceof BatchIterator ) {
            AlgNode routedIterator = dmlRouter.handleBatchIterator( logicalRoot.alg, statement, queryInformation );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedIterator, logicalRoot, queryInformation.getQueryClass() ) );
        } else if ( logicalRoot.alg instanceof ConstraintEnforcer ) {
            AlgNode routedConstraintEnforcer = dmlRouter.handleConstraintEnforcer( logicalRoot.alg, statement, queryInformation );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedConstraintEnforcer, logicalRoot, queryInformation.getQueryClass() ) );
        } else {
            final List<ProposedRoutingPlan> proposedPlans = new ArrayList<>();
            if ( statement.getTransaction().isAnalyze() ) {
                statement.getRoutingDuration().start( "Plan Proposing" );
            }

            for ( Router router : RoutingManager.getInstance().getRouters() ) {
                List<RoutedAlgBuilder> builders = router.route( logicalRoot, statement, queryInformation );
                List<ProposedRoutingPlan> plans = builders.stream()
                        .map( builder -> new ProposedRoutingPlanImpl( builder, logicalRoot, queryInformation.getQueryClass(), router.getClass() ) )
                        .collect( Collectors.toList() );
                proposedPlans.addAll( plans );
            }

            if ( statement.getTransaction().isAnalyze() ) {
                statement.getRoutingDuration().stop( "Plan Proposing" );
                statement.getRoutingDuration().start( "Remove Duplicates" );
            }

            final List<ProposedRoutingPlan> distinctPlans = proposedPlans.stream().distinct().collect( Collectors.toList() );

            if ( distinctPlans.isEmpty() ) {
                throw new RuntimeException( "No routing of query found" );
            }

            if ( statement.getTransaction().isAnalyze() ) {
                statement.getRoutingDuration().stop( "Remove Duplicates" );
            }

            return distinctPlans;
        }
    }


    private List<ProposedRoutingPlan> routeGraph( AlgRoot logicalRoot, LogicalQueryInformation queryInformation, DmlRouter dmlRouter ) {
        if ( logicalRoot.alg instanceof LogicalLpgModify ) {
            AlgNode routedDml = dmlRouter.routeGraphDml( (LogicalLpgModify) logicalRoot.alg, statement );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedDml, logicalRoot, queryInformation.getQueryClass() ) );
        }
        RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, logicalRoot.alg.getCluster() );
        AlgNode node = RoutingManager.getInstance().getRouters().get( 0 ).routeGraph( builder, (AlgNode & LpgAlg) logicalRoot.alg, statement );
        return Lists.newArrayList( new ProposedRoutingPlanImpl( builder.stackSize() == 0 ? node : builder.build(), logicalRoot, queryInformation.getQueryClass() ) );
    }


    private List<ProposedRoutingPlan> routeDocument( AlgRoot logicalRoot, LogicalQueryInformation queryInformation, DmlRouter dmlRouter ) {
        if ( logicalRoot.alg instanceof LogicalDocumentModify ) {
            AlgNode routedDml = dmlRouter.routeDocumentDml( (LogicalDocumentModify) logicalRoot.alg, statement, queryInformation, null );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedDml, logicalRoot, queryInformation.getQueryClass() ) );
        }
        RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, logicalRoot.alg.getCluster() );
        AlgNode node = RoutingManager.getInstance().getRouters().get( 0 ).routeDocument( builder, (AlgNode & DocumentAlg) logicalRoot.alg, statement );
        return Lists.newArrayList( new ProposedRoutingPlanImpl( builder.stackSize() == 0 ? node : builder.build(), logicalRoot, queryInformation.getQueryClass() ) );
    }


    private List<ProposedRoutingPlan> routeCached( AlgRoot logicalRoot, List<CachedProposedRoutingPlan> routingPlansCached, Statement statement, LogicalQueryInformation queryInformation, boolean isAnalyze ) {
        if ( isAnalyze ) {
            statement.getRoutingDuration().start( "Select Cached Plan" );
        }

        CachedProposedRoutingPlan selectedCachedPlan = selectCachedPlan( routingPlansCached );

        if ( isAnalyze ) {
            statement.getRoutingDuration().stop( "Select Cached Plan" );
        }

        if ( isAnalyze ) {
            statement.getRoutingDuration().start( "Route Cached Plan" );
        }

        RoutedAlgBuilder builder = RoutingManager.getInstance().getCachedPlanRouter().routeCached(
                logicalRoot,
                selectedCachedPlan,
                statement,
                queryInformation );

        if ( isAnalyze ) {
            statement.getRoutingDuration().stop( "Route Cached Plan" );
            statement.getRoutingDuration().start( "Create Plan From Cache" );
        }

        ProposedRoutingPlan proposed = new ProposedRoutingPlanImpl( builder, logicalRoot, queryInformation.getQueryClass(), selectedCachedPlan );

        if ( isAnalyze ) {
            statement.getRoutingDuration().stop( "Create Plan From Cache" );
        }

        return Lists.newArrayList( proposed );
    }


    private Pair<AlgRoot, AlgDataType> parameterize( AlgRoot routedRoot, AlgDataType parameterRowType ) {
        AlgNode routed = routedRoot.alg;
        List<AlgDataType> parameterRowTypeList = new ArrayList<>();
        parameterRowType.getFieldList().forEach( algDataTypeField -> parameterRowTypeList.add( algDataTypeField.getType() ) );

        // Parameterize
        QueryParameterizer queryParameterizer = new QueryParameterizer( parameterRowType.getFieldCount(), parameterRowTypeList, routed.getTraitSet().contains( ModelTrait.GRAPH ) );
        AlgNode parameterized = routed.accept( queryParameterizer );
        List<AlgDataType> types = queryParameterizer.getTypes();

        if ( routed.getTraitSet().contains( ModelTrait.GRAPH ) ) {
            // we are not as strict in the context when dealing with graph queries/mixed-model queries
            statement.getDataContext().setMixedModel( true );
            // graph updates are not symmetric and need special logic to allow to be parameterized
            for ( Map<Integer, List<ParameterValue>> value : queryParameterizer.getDocs().values() ) {
                // Add values to data context
                for ( List<DataContext.ParameterValue> values : value.values() ) {
                    List<Object> o = new ArrayList<>();
                    for ( ParameterValue v : values ) {
                        o.add( v.getValue() );
                    }
                    statement.getDataContext().addParameterValues( values.get( 0 ).getIndex(), values.get( 0 ).getType(), o );
                }

                statement.getDataContext().addContext();
            }
            statement.getDataContext().resetContext();
        } else {
            // Add values to data context
            for ( List<DataContext.ParameterValue> values : queryParameterizer.getValues().values() ) {
                List<Object> o = new ArrayList<>();
                for ( ParameterValue v : values ) {
                    o.add( v.getValue() );
                }
                statement.getDataContext().addParameterValues( values.get( 0 ).getIndex(), values.get( 0 ).getType(), o );
            }
        }

        // parameterRowType
        AlgDataType newParameterRowType = statement.getTransaction().getTypeFactory().createStructType(
                types,
                new AbstractList<String>() {
                    @Override
                    public String get( int index ) {
                        return "?" + index;
                    }


                    @Override
                    public int size() {
                        return types.size();
                    }
                } );

        return new Pair<>(
                new AlgRoot( parameterized, routedRoot.validatedRowType, routedRoot.kind, routedRoot.fields, routedRoot.collation ),
                newParameterRowType
        );
    }


    private AlgNode optimize( AlgRoot logicalRoot, Convention resultConvention ) {
        AlgNode logicalPlan = logicalRoot.alg;

        final AlgTraitSet desiredTraits = logicalPlan.getTraitSet()
                .replace( resultConvention )
                .replace( algCollation( logicalPlan ) )
                .simplify();

        final Program program = Programs.standard();
        return program.run( getPlanner(), logicalPlan, desiredTraits );
    }


    private PreparedResult implement( AlgRoot root, AlgDataType parameterRowType ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "Physical query plan: [{}]", AlgOptUtil.dumpPlan( "-- Physical Plan", root.alg, ExplainFormat.TEXT, ExplainLevel.DIGEST_ATTRIBUTES ) );
        }

        final AlgDataType jdbcType = QueryProcessorHelpers.makeStruct( root.alg.getCluster().getTypeFactory(), root.validatedRowType );
        List<List<String>> fieldOrigins = Collections.nCopies( jdbcType.getFieldCount(), null );

        final Prefer prefer = Prefer.ARRAY;
        final Convention resultConvention =
                ENABLE_BINDABLE
                        ? BindableConvention.INSTANCE
                        : EnumerableConvention.INSTANCE;

        final Bindable<Object[]> bindable;
        final String generatedCode;
        if ( resultConvention == BindableConvention.INSTANCE ) {
            bindable = Interpreters.bindable( root.alg );
            generatedCode = null;
        } else {
            EnumerableAlg enumerable = (EnumerableAlg) root.alg;
            if ( !root.isRefTrivial() ) {
                final List<RexNode> projects = new ArrayList<>();
                final RexBuilder rexBuilder = enumerable.getCluster().getRexBuilder();
                for ( int field : Pair.left( root.fields ) ) {
                    projects.add( rexBuilder.makeInputRef( enumerable, field ) );
                }
                RexProgram program = RexProgram.create( enumerable.getRowType(), projects, null, root.validatedRowType, rexBuilder );
                enumerable = EnumerableCalc.create( enumerable, program );
            }

            try {
                CatalogReader.THREAD_LOCAL.set( statement.getTransaction().getCatalogReader() );
                final Conformance conformance = statement.getPrepareContext().config().conformance();

                final Map<String, Object> internalParameters = new LinkedHashMap<>();
                internalParameters.put( "_conformance", conformance );

                Pair<Bindable<Object[]>, String> implementationPair = EnumerableInterpretable.toBindable(
                        internalParameters,
                        enumerable,
                        prefer,
                        statement );
                bindable = implementationPair.left;
                generatedCode = implementationPair.right;
                statement.getDataContext().addAll( internalParameters );
            } finally {
                CatalogReader.THREAD_LOCAL.remove();
            }
        }

        AlgDataType resultType = root.alg.getRowType();
        boolean isDml = root.kind.belongsTo( Kind.DML );

        return new PreparedResultImpl(
                resultType,
                parameterRowType,
                fieldOrigins,
                root.collation.getFieldCollations().isEmpty()
                        ? ImmutableList.of()
                        : ImmutableList.of( root.collation ),
                root.alg,
                QueryProcessorHelpers.mapTableModOp( isDml, root.kind ),
                isDml ) {
            @Override
            public String getCode() {
                return generatedCode;
            }


            @Override
            public Bindable<Object[]> getBindable( CursorFactory cursorFactory ) {
                return bindable;
            }


            @Override
            public Type getElementType() {
                return ((Typed) bindable).getElementType();
            }
        };
    }


    private AlgCollation algCollation( AlgNode node ) {
        return node instanceof Sort
                ? ((Sort) node).collation
                : AlgCollations.EMPTY;
    }


    private PolyImplementation createPolyImplementation( PreparedResult preparedResult, Kind kind, AlgNode optimalNode, AlgDataType validatedRowType, Convention resultConvention, ExecutionTimeMonitor executionTimeMonitor, NamespaceType namespaceType ) {
        final AlgDataType jdbcType = QueryProcessorHelpers.makeStruct( optimalNode.getCluster().getTypeFactory(), validatedRowType );
        return new PolyImplementation(
                jdbcType,
                namespaceType,
                executionTimeMonitor,
                preparedResult,
                kind,
                statement,
                resultConvention
        );
    }


    private boolean isQueryPlanCachingActive( Statement statement, AlgRoot algRoot ) {
        return RuntimeConfig.QUERY_PLAN_CACHING.getBoolean()
                && statement.getTransaction().getUseCache()
                && (!algRoot.kind.belongsTo( Kind.DML ) || RuntimeConfig.QUERY_PLAN_CACHING_DML.getBoolean() || statement.getDataContext().getParameterValues().size() > 0);
    }


    private boolean isImplementationCachingActive( Statement statement, AlgRoot algRoot ) {
        return RuntimeConfig.IMPLEMENTATION_CACHING.getBoolean()
                && statement.getTransaction().getUseCache()
                && (!algRoot.kind.belongsTo( Kind.DML ) || RuntimeConfig.IMPLEMENTATION_CACHING_DML.getBoolean() || statement.getDataContext().getParameterValues().size() > 0);
    }


    private LogicalQueryInformation analyzeQueryAndPrepareMonitoring( Statement statement, AlgRoot logicalRoot, boolean isAnalyze, boolean isSubquery ) {
        // Analyze logical query
        LogicalAlgAnalyzeShuttle analyzeRelShuttle = new LogicalAlgAnalyzeShuttle( statement );
        logicalRoot.alg.accept( analyzeRelShuttle );

        // Get partitions of logical information
        Map<Integer, Set<String>> partitionValueFilterPerScan = analyzeRelShuttle.getPartitionValueFilterPerScan();
        Map<Integer, List<Long>> accessedPartitionMap = this.getAccessedPartitionsPerScan( logicalRoot.alg, partitionValueFilterPerScan );

        // Build queryClass from query-name and partitions.
        String queryClass = analyzeRelShuttle.getQueryName();// + accessedPartitionMap;

        // Build LogicalQueryInformation instance and prepare monitoring
        LogicalQueryInformation queryInformation = new LogicalQueryInformationImpl(
                queryClass,
                accessedPartitionMap,
                analyzeRelShuttle.availableColumns,
                analyzeRelShuttle.availableColumnsWithTable,
                analyzeRelShuttle.getUsedColumns(),
                analyzeRelShuttle.getEntities() );
        this.prepareMonitoring( statement, logicalRoot, isAnalyze, isSubquery, queryInformation );

        return queryInformation;
    }


    /**
     * Traverses all TablesScans used during execution and identifies for the corresponding table all
     * associated partitions that needs to be accessed, on the basis of the provided partitionValues identified in a LogicalFilter
     *
     * It is necessary to associate the partitionIds again with the ScanId and not with the table itself. Because a table could be present
     * multiple times within one query. The aggregation per table would lead to data loss
     *
     * @param alg AlgNode to be processed
     * @param aggregatedPartitionValues Mapping of Scan Ids to identified partition Values
     * @return Mapping of Scan Ids to identified partition Ids
     */
    private Map<Integer, List<Long>> getAccessedPartitionsPerScan( AlgNode alg, Map<Integer, Set<String>> aggregatedPartitionValues ) {
        Map<Integer, List<Long>> accessedPartitionList = new HashMap<>(); // tableId  -> partitionIds
        if ( !(alg instanceof LogicalScan) ) {
            for ( int i = 0; i < alg.getInputs().size(); i++ ) {
                Map<Integer, List<Long>> result = getAccessedPartitionsPerScan( alg.getInput( i ), aggregatedPartitionValues );
                if ( !result.isEmpty() ) {
                    for ( Map.Entry<Integer, List<Long>> elem : result.entrySet() ) {
                        accessedPartitionList.merge( elem.getKey(), elem.getValue(), ( l1, l2 ) -> Stream.concat( l1.stream(), l2.stream() ).collect( Collectors.toList() ) );
                    }
                }
            }
        } else {
            boolean fallback = false;
            if ( alg.getTable() != null ) {
                AlgOptTableImpl table = (AlgOptTableImpl) alg.getTable();
                if ( table.getTable() instanceof LogicalTable ) {
                    LogicalTable logicalTable = ((LogicalTable) table.getTable());
                    int scanId = alg.getId();

                    if ( logicalTable.getTableId() == -1 ) {
                        // todo dl: remove after RowType refactor
                        return accessedPartitionList;
                    }

                    // Get placements of this table
                    CatalogTable catalogTable = Catalog.getInstance().getTable( logicalTable.getTableId() );

                    if ( aggregatedPartitionValues.containsKey( scanId ) ) {
                        if ( aggregatedPartitionValues.get( scanId ) != null ) {
                            if ( !aggregatedPartitionValues.get( scanId ).isEmpty() ) {
                                List<String> partitionValues = new ArrayList<>( aggregatedPartitionValues.get( scanId ) );

                                if ( log.isDebugEnabled() ) {
                                    log.debug(
                                            "TableID: {} is partitioned on column: {} - {}",
                                            logicalTable.getTableId(),
                                            catalogTable.partitionProperty.partitionColumnId,
                                            Catalog.getInstance().getColumn( catalogTable.partitionProperty.partitionColumnId ).name );
                                }
                                List<Long> identifiedPartitions = new ArrayList<>();
                                for ( String partitionValue : partitionValues ) {
                                    if ( log.isDebugEnabled() ) {
                                        log.debug( "Extracted PartitionValue: {}", partitionValue );
                                    }
                                    long identifiedPartition = PartitionManagerFactory.getInstance()
                                            .getPartitionManager( catalogTable.partitionProperty.partitionType )
                                            .getTargetPartitionId( catalogTable, partitionValue );

                                    identifiedPartitions.add( identifiedPartition );
                                    if ( log.isDebugEnabled() ) {
                                        log.debug( "Identified PartitionId: {} for value: {}", identifiedPartition, partitionValue );
                                    }
                                }

                                accessedPartitionList.merge(
                                        scanId,
                                        identifiedPartitions,
                                        ( l1, l2 ) -> Stream.concat( l1.stream(), l2.stream() ).collect( Collectors.toList() ) );
                                scanPerTable.putIfAbsent( scanId, catalogTable.id );
                                // Fallback all partitionIds are needed
                            } else {
                                fallback = true;
                            }
                        } else {
                            fallback = true;
                        }
                    } else {
                        fallback = true;
                    }

                    if ( fallback ) {
                        accessedPartitionList.merge(
                                scanId,
                                catalogTable.partitionProperty.partitionIds,
                                ( l1, l2 ) -> Stream.concat( l1.stream(), l2.stream() ).collect( Collectors.toList() ) );
                        scanPerTable.putIfAbsent( scanId, catalogTable.id );
                    }
                }
            }
        }
        return accessedPartitionList;
    }


    private void prepareMonitoring( Statement statement, AlgRoot logicalRoot, boolean isAnalyze, boolean isSubquery, LogicalQueryInformation queryInformation ) {

        // Initialize Monitoring
        if ( statement.getMonitoringEvent() == null ) {
            StatementEvent event;
            if ( logicalRoot.kind.belongsTo( Kind.DML ) ) {
                event = new DmlEvent();
            } else if ( logicalRoot.kind.belongsTo( Kind.QUERY ) ) {
                event = new QueryEvent();
            } else {
                log.error( "No corresponding monitoring event class found" );
                event = new QueryEvent();
            }

            event.setAnalyze( isAnalyze );
            event.setSubQuery( isSubquery );
            event.setLogicalQueryInformation( queryInformation );
            event.setMonitoringType( logicalRoot.kind.name() );
            statement.setMonitoringEvent( event );
        }
    }


    private void monitorResult( ProposedRoutingPlan selectedPlan ) {
        if ( statement.getMonitoringEvent() != null ) {
            StatementEvent eventData = statement.getMonitoringEvent();
            eventData.setAlgCompareString( selectedPlan.getRoutedRoot().alg.algCompareString() );
            if ( selectedPlan.getPhysicalQueryClass() != null ) {
                eventData.setPhysicalQueryClass( selectedPlan.getPhysicalQueryClass() );
                //eventData.setRowCount( (int) selectedPlan.getRoutedRoot().alg.estimateRowCount( selectedPlan.getRoutedRoot().alg.getCluster().getMetadataQuery() ) );
            }

            if ( RoutingManager.POST_COST_AGGREGATION_ACTIVE.getBoolean() ) {
                if ( eventData instanceof QueryEvent ) {
                    // aggregate post costs
                    ((QueryEvent) eventData).setUpdatePostCosts( true );
                }
            }
            finalizeAccessedPartitions( eventData );
        }
    }


    /**
     * Aggregates results present in queryInformation as well information directly attached to the Statement Event
     * Adds all information to teh accessedPartitions directly in the StatementEvent.
     *
     * Also remaps scanId to tableId to correctly update the accessed partition List
     *
     * @param eventData monitoring data to be updated
     */
    private void finalizeAccessedPartitions( StatementEvent eventData ) {
        Map<Integer, List<Long>> partitionsInQueryInformation = eventData.getLogicalQueryInformation().getAccessedPartitions();
        Map<Long, Set<Long>> tempAccessedPartitions = new HashMap<>();

        for ( Entry<Integer, List<Long>> entry : partitionsInQueryInformation.entrySet() ) {
            Integer scanId = entry.getKey();
            if ( scanPerTable.containsKey( scanId ) ) {
                Set<Long> partitionIds = new HashSet<>( entry.getValue() );

                long tableId = scanPerTable.get( scanId );
                tempAccessedPartitions.put( tableId, partitionIds );

                eventData.updateAccessedPartitions( tempAccessedPartitions );
            }
        }

        // Otherwise, Analyzer cannot correctly analyze the event anymore
        if ( eventData.getAccessedPartitions() == null ) {
            eventData.setAccessedPartitions( Collections.emptyMap() );
        }

    }


    private void cacheRouterPlans( List<ProposedRoutingPlan> proposedRoutingPlans, List<AlgOptCost> approximatedCosts, String queryId, Set<Long> partitionIds ) {
        List<CachedProposedRoutingPlan> cachedPlans = new ArrayList<>();
        for ( int i = 0; i < proposedRoutingPlans.size(); i++ ) {
            if ( proposedRoutingPlans.get( i ).isCacheable() && !RoutingPlanCache.INSTANCE.isKeyPresent( queryId, partitionIds ) ) {
                cachedPlans.add( new CachedProposedRoutingPlan( proposedRoutingPlans.get( i ), approximatedCosts.get( i ) ) );
            }
        }

        if ( !cachedPlans.isEmpty() ) {
            RoutingPlanCache.INSTANCE.put( queryId, partitionIds, cachedPlans );
        }
    }


    private Pair<PolyImplementation, ProposedRoutingPlan> selectPlan( ProposedImplementations proposedImplementations ) {
        // Lists should all be same size
        List<ProposedRoutingPlan> proposedRoutingPlans = proposedImplementations.getProposedRoutingPlans();
        List<AlgNode> optimalAlgs = proposedImplementations.getOptimizedPlans();
        List<PolyImplementation> results = proposedImplementations.getResults();
        List<String> generatedCodes = proposedImplementations.getGeneratedCodes();
        LogicalQueryInformation queryInformation = proposedImplementations.getLogicalQueryInformation();

        List<AlgOptCost> approximatedCosts;
        if ( RuntimeConfig.ROUTING_PLAN_CACHING.getBoolean() ) {
            // Get approximated costs and cache routing plans
            approximatedCosts = optimalAlgs.stream()
                    .map( alg -> alg.computeSelfCost( getPlanner(), alg.getCluster().getMetadataQuery() ) )
                    .collect( Collectors.toList() );
            this.cacheRouterPlans(
                    proposedRoutingPlans,
                    approximatedCosts,
                    queryInformation.getQueryClass(),
                    queryInformation.getAccessedPartitions().values().stream().flatMap( List::stream ).collect( Collectors.toSet() ) );
        }

        if ( results.size() == 1 ) {
            // If only one plan proposed, return this without further selection
            if ( statement.getTransaction().isAnalyze() ) {
                UiRoutingPageUtil.outputSingleResult(
                        proposedRoutingPlans.get( 0 ),
                        optimalAlgs.get( 0 ),
                        statement.getTransaction().getQueryAnalyzer() );
                addGeneratedCodeToQueryAnalyzer( generatedCodes.get( 0 ) );
            }
            return new Pair<>( results.get( 0 ), proposedRoutingPlans.get( 0 ) );
        } else {
            // Calculate costs and get selected plan from plan selector
            approximatedCosts = optimalAlgs.stream()
                    .map( alg -> alg.computeSelfCost( getPlanner(), alg.getCluster().getMetadataQuery() ) )
                    .collect( Collectors.toList() );
            RoutingPlan routingPlan = RoutingManager.getInstance().getRoutingPlanSelector().selectPlanBasedOnCosts(
                    proposedRoutingPlans,
                    approximatedCosts,
                    statement );

            int index = proposedRoutingPlans.indexOf( (ProposedRoutingPlan) routingPlan );
            if ( statement.getTransaction().isAnalyze() ) {
                AlgNode optimalNode = optimalAlgs.get( index );
                UiRoutingPageUtil.addPhysicalPlanPage( optimalNode, statement.getTransaction().getQueryAnalyzer() );
                addGeneratedCodeToQueryAnalyzer( generatedCodes.get( index ) );
            }
            return new Pair<>( proposedImplementations.getResults().get( index ), (ProposedRoutingPlan) routingPlan );
        }
    }


    private void addGeneratedCodeToQueryAnalyzer( String code ) {
        if ( code != null ) {
            InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "Implementation" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Java Code" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationCode informationCode = new InformationCode( group, code );
            queryAnalyzer.registerInformation( informationCode );
        } else {
            log.error( "Generated code is null" );
        }
    }


    private CachedProposedRoutingPlan selectCachedPlan( List<CachedProposedRoutingPlan> routingPlansCached ) {
        if ( routingPlansCached.size() == 1 ) {
            return routingPlansCached.get( 0 );
        }

        List<AlgOptCost> approximatedCosts = routingPlansCached.stream()
                .map( CachedProposedRoutingPlan::getPreCosts )
                .collect( Collectors.toList() );
        RoutingPlan routingPlan = RoutingManager.getInstance().getRoutingPlanSelector().selectPlanBasedOnCosts(
                routingPlansCached,
                approximatedCosts,
                statement );

        return (CachedProposedRoutingPlan) routingPlan;
    }


    @Override
    public void unlock( Statement statement ) {
        LockManager.INSTANCE.unlock( List.of( LockManager.GLOBAL_LOCK ), (TransactionImpl) statement.getTransaction() );
    }


    /**
     * To acquire a global shared lock for a statement.
     * This method is used before the statistics are updated to make sure nothing changes during the updating process.
     */
    @Override
    public void lock( Statement statement ) {
        try {
            LockManager.INSTANCE.lock( Collections.singletonList( Pair.of( LockManager.GLOBAL_LOCK, LockMode.SHARED ) ), (TransactionImpl) statement.getTransaction() );
        } catch ( DeadlockException e ) {
            throw new RuntimeException( "DeadLock while locking to reevaluate statistics", e );
        }
    }

}
