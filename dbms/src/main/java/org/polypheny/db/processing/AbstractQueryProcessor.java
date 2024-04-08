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

package org.polypheny.db.processing;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.lang.reflect.Type;
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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.commons.lang3.time.StopWatch;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataContext.ParameterValue;
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
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlg.Prefer;
import org.polypheny.db.algebra.enumerable.EnumerableCalc;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.EnumerableInterpretable;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.Information;
import org.polypheny.db.information.InformationCode;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.interpreter.Interpreters;
import org.polypheny.db.monitoring.events.DmlEvent;
import org.polypheny.db.monitoring.events.MonitoringType;
import org.polypheny.db.monitoring.events.QueryEvent;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.Prepare.PreparedResult;
import org.polypheny.db.prepare.Prepare.PreparedResultImpl;
import org.polypheny.db.processing.caching.ImplementationCache;
import org.polypheny.db.processing.caching.QueryPlanCache;
import org.polypheny.db.processing.caching.RoutingPlanCache;
import org.polypheny.db.processing.shuttles.LogicalQueryInformationImpl;
import org.polypheny.db.processing.shuttles.ParameterValueValidator;
import org.polypheny.db.processing.shuttles.QueryParameterizer;
import org.polypheny.db.processing.util.Plan;
import org.polypheny.db.processing.util.ProposedImplementations;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.routing.DmlRouter;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.routing.ExecutionTimeMonitor.ExecutionTimeObserver;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.routing.ProposedRoutingPlan;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.RoutingContext;
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.routing.RoutingPlan;
import org.polypheny.db.routing.UiRoutingPageUtil;
import org.polypheny.db.routing.dto.CachedProposedRoutingPlan;
import org.polypheny.db.routing.dto.ProposedRoutingPlanImpl;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.Typed;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.trait.ModelTraitDef;
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
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.view.MaterializedViewManager;


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
    private final Map<Long, Long> scanPerTable = new HashMap<>();


    protected AbstractQueryProcessor( Statement statement ) {
        this.statement = statement;
    }


    @Override
    public void executionTime( String reference, long nanoTime ) {
        StatementEvent event = statement.getMonitoringEvent();
        if ( reference.equals( event.getLogicalQueryInformation().getQueryHash() ) ) {
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
            attachQueryPlans( logicalRoot );
        }

        if ( statement.getTransaction().isAnalyze() ) {
            statement.getOverviewDuration().start( "Processing" );
        }
        final ProposedImplementations proposedImplementations = prepareQueries( logicalRoot, parameterRowType, isRouted, isSubquery );

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


    private void attachQueryPlans( AlgRoot logicalRoot ) {
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


    private ProposedImplementations prepareQueries( AlgRoot logicalRoot, AlgDataType parameterRowType, boolean isRouted, boolean isSubQuery ) {
        boolean isAnalyze = statement.getTransaction().isAnalyze() && !isSubQuery;
        boolean lock = !isSubQuery;

        final Convention resultConvention = ENABLE_BINDABLE ? BindableConvention.INSTANCE : EnumerableConvention.INSTANCE;
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        // Initialize result lists. They will all be with in the same ordering.
        List<Plan> plans = null;

        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Expand Views" );
        }

        // Check for view
        if ( logicalRoot.info.containsView ) {
            logicalRoot = logicalRoot.unfoldView();
        }

        // Analyze step
        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Expand Views" );
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
            executionTimeMonitor.subscribe( this, logicalQueryInformation.getQueryHash() );
        }

        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Expand Views" );
        }

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
            plans = List.of( new Plan().proposedRoutingPlan( new ProposedRoutingPlanImpl( logicalRoot, logicalQueryInformation.getQueryHash() ) ) );
        } else {
            // Locking
            if ( isAnalyze ) {
                statement.getProcessingDuration().start( "Locking" );
            }
            if ( lock ) {
                this.acquireLock( isAnalyze, logicalRoot, logicalQueryInformation.getAccessedPartitions() );
            }

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
            plans = routePlans( indexLookupRoot, logicalQueryInformation, plans, isAnalyze );

            if ( isAnalyze ) {
                statement.getRoutingDuration().start( "Flattener" );
            }

            flattenPlans( plans );

            if ( isAnalyze ) {
                statement.getRoutingDuration().stop( "Flattener" );
                statement.getProcessingDuration().stop( "Routing" );
            }
        }

        // Parameterize
        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Parameterize" );
        }

        // Add optional parameterizedRoots and results for all routed AlgRoots.
        // Index of routedRoot, parameterizedRootList and results correspond!
        for ( Plan plan : plans ) {
            AlgRoot routedRoot = plan.proposedRoutingPlan().getRoutedRoot();
            AlgRoot parameterizedRoot;
            if ( statement.getDataContext().getParameterValues().isEmpty()
                    && (RuntimeConfig.PARAMETERIZE_DML.getBoolean() || !routedRoot.kind.belongsTo( Kind.DML )) ) {
                Pair<AlgRoot, AlgDataType> parameterized = parameterize( routedRoot, parameterRowType );
                parameterizedRoot = parameterized.left;
            } else {
                // This query is an execution of a prepared statement
                parameterizedRoot = routedRoot;
            }

            plan.parameterizedRoot( parameterizedRoot );
        }

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Parameterize" );
        }

        // Implementation Caching
        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Implementation Caching" );
        }

        for ( Plan plan : plans ) {
            AlgRoot routedRoot = plan.proposedRoutingPlan().getRoutedRoot();
            if ( this.isImplementationCachingActive( statement, routedRoot ) ) {
                AlgRoot parameterizedRoot = plan.parameterizedRoot();
                PreparedResult<PolyValue> preparedResult = ImplementationCache.INSTANCE.getIfPresent( parameterizedRoot.alg );
                AlgNode optimalNode = QueryPlanCache.INSTANCE.getIfPresent( parameterizedRoot.alg );
                if ( preparedResult != null ) {
                    PolyImplementation result = createPolyImplementation(
                            preparedResult,
                            parameterizedRoot.kind,
                            optimalNode,
                            parameterizedRoot.validatedRowType,
                            resultConvention,
                            executionTimeMonitor,
                            Objects.requireNonNull( optimalNode.getTraitSet().getTrait( ModelTraitDef.INSTANCE ) ).dataModel() );
                    plan.result( result );
                    plan.generatedCodes( preparedResult.getCode() );
                    plan.optimalNode( optimalNode );
                }
            }
        }

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Implementation Caching" );
        }

        // Can we return earlier?
        if ( plans.stream().allMatch( obj -> Objects.nonNull( obj.result() ) && Objects.nonNull( obj.optimalNode() ) ) ) {
            return new ProposedImplementations( plans.stream().filter( Plan::isValid ).toList(), logicalQueryInformation );
        }

        //
        // Plan Caching
        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Plan Caching" );
        }
        for ( Plan plan : plans ) {
            if ( this.isQueryPlanCachingActive( statement, plan.proposedRoutingPlan().getRoutedRoot() ) ) {
                // Should always be the case
                AlgNode cachedElem = QueryPlanCache.INSTANCE.getIfPresent( plan.parameterizedRoot().alg );
                if ( cachedElem != null ) {
                    plan.optimalNode( cachedElem );
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
        for ( Plan plan : plans ) {
            if ( plan.optimalNode() != null ) {
                continue;
            }
            plan.optimalNode( optimize( plan.parameterizedRoot(), resultConvention ) );

            if ( this.isQueryPlanCachingActive( statement, plan.proposedRoutingPlan().getRoutedRoot() ) ) {
                QueryPlanCache.INSTANCE.put( plan.parameterizedRoot().alg, plan.optimalNode() );
            }
        }

        //
        // Implementation
        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Planning & Optimization" );
            statement.getProcessingDuration().start( "Implementation" );
        }

        for ( Plan plan : plans ) {
            if ( plan.result() != null ) {
                continue;
            }

            final AlgDataType rowType = plan.parameterizedRoot().alg.getTupleType();
            final List<Pair<Integer, String>> fields = Pair.zip( PolyTypeUtil.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
            AlgRoot optimalRoot = new AlgRoot( plan.optimalNode(), rowType, plan.parameterizedRoot().kind, fields, algCollation( plan.parameterizedRoot().alg ) );

            PreparedResult<PolyValue> preparedResult = implement( optimalRoot, parameterRowType );

            // Cache implementation
            if ( this.isImplementationCachingActive( statement, plan.proposedRoutingPlan().getRoutedRoot() ) ) {
                if ( optimalRoot.alg.isImplementationCacheable() ) {
                    ImplementationCache.INSTANCE.put( plan.parameterizedRoot().alg, preparedResult );
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
                    Objects.requireNonNull( plan.optimalNode().getTraitSet().getTrait( ModelTraitDef.INSTANCE ) ).dataModel() );
            plan.result( result );
            plan.generatedCodes( preparedResult.getCode() );
            plan.optimalNode( optimalRoot.alg );
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
                plans.stream().filter( Plan::isValid ).toList(),
                logicalQueryInformation );
    }


    @NotNull
    private List<Plan> routePlans( AlgRoot indexLookupRoot, LogicalQueryInformation logicalQueryInformation, List<Plan> plans, boolean isAnalyze ) {
        if ( RuntimeConfig.ROUTING_PLAN_CACHING.getBoolean() && !indexLookupRoot.kind.belongsTo( Kind.DML ) ) {
            Set<Long> partitionIds = logicalQueryInformation.getAccessedPartitions().values().stream()
                    .flatMap( List::stream )
                    .collect( Collectors.toSet() );
            List<CachedProposedRoutingPlan> routingPlansCached = RoutingPlanCache.INSTANCE.getIfPresent( logicalQueryInformation.getQueryHash(), partitionIds );
            if ( !routingPlansCached.isEmpty() && routingPlansCached.stream().noneMatch( Objects::nonNull ) ) {
                plans = routeCached( indexLookupRoot, routingPlansCached, new RoutingContext( indexLookupRoot.alg.getCluster(), statement, logicalQueryInformation ), isAnalyze );
            }
        }

        if ( plans == null ) {
            plans = route( indexLookupRoot, statement, logicalQueryInformation ).stream().map( p -> new Plan().proposedRoutingPlan( p ) ).toList();
        }
        return plans;
    }


    private void flattenPlans( List<Plan> plans ) {
        for ( Plan plan : plans ) {
            AlgRoot routedRoot = plan.proposedRoutingPlan().getRoutedRoot();
            AlgStructuredTypeFlattener typeFlattener = new AlgStructuredTypeFlattener(
                    AlgBuilder.create( statement, routedRoot.alg.getCluster() ),
                    routedRoot.alg.getCluster().getRexBuilder(),
                    routedRoot.alg.getCluster(),
                    true );
            plan.proposedRoutingPlan().setRoutedRoot( routedRoot.withAlg( typeFlattener.rewrite( routedRoot.alg ) ) );
        }
    }


    private void acquireLock( boolean isAnalyze, AlgRoot logicalRoot, Map<Long, List<Long>> accessedPartitions ) {
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
            throw new GenericRuntimeException( e );
        }
    }


    private AlgRoot indexUpdate( AlgRoot root, Statement statement, AlgDataType parameterRowType ) {
        if ( root.kind.belongsTo( Kind.DML ) ) {
            final AlgShuttle shuttle = new AlgShuttleImpl() {

                @Override
                public AlgNode visit( AlgNode node ) {
                    RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
                    if ( node instanceof LogicalRelModify ltm ) {
                        final Catalog catalog = Catalog.getInstance();
                        final LogicalTable table = ltm.getEntity().unwrap( LogicalTable.class ).orElseThrow();
                        final LogicalNamespace namespace = catalog.getSnapshot().getNamespace( table.namespaceId ).orElseThrow();
                        final List<Index> indices = IndexManager.getInstance().getIndices( namespace, table );

                        // Check if there are any indexes effected by this table modify
                        if ( indices.isEmpty() ) {
                            // Nothing to do here
                            return super.visit( node );
                        }

                        if ( ltm.isInsert() && ltm.getInput() instanceof Values ) {
                            final LogicalRelValues lvalues = (LogicalRelValues) ltm.getInput( 0 ).accept( new DeepCopyShuttle() );
                            for ( final Index index : indices ) {
                                final Set<Pair<List<PolyValue>, List<PolyValue>>> tuplesToInsert = new HashSet<>( lvalues.tuples.size() );
                                for ( final ImmutableList<RexLiteral> row : lvalues.getTuples() ) {
                                    final List<PolyValue> rowValues = new ArrayList<>();
                                    final List<PolyValue> targetRowValues = new ArrayList<>();
                                    for ( final String column : index.getColumns() ) {
                                        final RexLiteral fieldValue = row.get(
                                                lvalues.getTupleType().getField( column, false, false ).getIndex()
                                        );
                                        rowValues.add( fieldValue.value );
                                    }
                                    for ( final String column : index.getTargetColumns() ) {
                                        final RexLiteral fieldValue = row.get(
                                                lvalues.getTupleType().getField( column, false, false ).getIndex()
                                        );
                                        targetRowValues.add( fieldValue.value );
                                    }
                                    tuplesToInsert.add( new Pair<>( rowValues, targetRowValues ) );
                                }
                                index.insertAll( statement.getTransaction().getXid(), tuplesToInsert );
                            }
                        } else if ( ltm.isInsert() && ltm.getInput() instanceof LogicalRelProject && ((LogicalRelProject) ltm.getInput()).getInput().getTupleType().toString().equals( "RecordType(INTEGER ZERO)" ) ) {
                            final LogicalRelProject lproject = (LogicalRelProject) ltm.getInput().accept( new DeepCopyShuttle() );
                            for ( final Index index : indices ) {
                                final Set<Pair<List<PolyValue>, List<PolyValue>>> tuplesToInsert = new HashSet<>( lproject.getProjects().size() );
                                final List<PolyValue> rowValues = new ArrayList<>();
                                final List<PolyValue> targetRowValues = new ArrayList<>();
                                for ( final String column : index.getColumns() ) {
                                    final RexNode fieldValue = lproject.getProjects().get(
                                            lproject.getTupleType().getField( column, false, false ).getIndex()
                                    );
                                    if ( fieldValue instanceof RexLiteral ) {
                                        rowValues.add( ((RexLiteral) fieldValue).value );
                                    } else if ( fieldValue instanceof RexDynamicParam ) {
                                        //
                                        // TODO: This is dynamic parameter. We need to do the index update in the generated code!
                                        //
                                        throw new GenericRuntimeException( "Index updates are not yet supported for prepared statements" );
                                    } else {
                                        throw new GenericRuntimeException( "Unexpected rex type: " + fieldValue.getClass() );
                                    }
                                }
                                for ( final String column : index.getTargetColumns() ) {
                                    final RexNode fieldValue = lproject.getProjects().get(
                                            lproject.getTupleType().getField( column, false, false ).getIndex()
                                    );
                                    if ( fieldValue instanceof RexLiteral ) {
                                        targetRowValues.add( ((RexLiteral) fieldValue).value );
                                    } else if ( fieldValue instanceof RexDynamicParam ) {
                                        //
                                        // TODO: This is dynamic parameter. We need to do the index update in the generated code!
                                        //
                                        throw new GenericRuntimeException( "Index updates are not yet supported for prepared statements" );
                                    } else {
                                        throw new GenericRuntimeException( "Unexpected rex type: " + fieldValue.getClass() );
                                    }
                                }
                                tuplesToInsert.add( new Pair<>( rowValues, targetRowValues ) );
                                index.insertAll( statement.getTransaction().getXid(), tuplesToInsert );
                            }
                        } else if ( ltm.isDelete() || ltm.isUpdate() || ltm.isMerge() || (ltm.isInsert() && !(ltm.getInput() instanceof Values)) ) {
                            final Map<String, Integer> nameMap = new HashMap<>();
                            final Map<String, Integer> newValueMap = new HashMap<>();
                            AlgNode original = ltm.getInput().accept( new DeepCopyShuttle() );
                            if ( !(original instanceof LogicalRelProject) ) {
                                original = LogicalRelProject.identity( original );
                            }
                            LogicalRelProject originalProject = (LogicalRelProject) original;

                            for ( int i = 0; i < originalProject.getNamedProjects().size(); ++i ) {
                                final Pair<RexNode, String> np = originalProject.getNamedProjects().get( i );
                                nameMap.put( np.right, i );
                                if ( ltm.isUpdate() || ltm.isMerge() ) {
                                    int j = ltm.getUpdateColumns().indexOf( np.right );
                                    if ( j >= 0 ) {
                                        RexNode newValue = ltm.getSourceExpressions().get( j );
                                        for ( int k = 0; k < originalProject.getNamedProjects().size(); ++k ) {
                                            if ( originalProject.getNamedProjects().get( k ).left.equals( newValue ) ) {
                                                newValueMap.put( np.right, k );
                                                break;
                                            }
                                        }
                                    }
                                } else if ( ltm.isInsert() ) {
                                    int j = originalProject.getTupleType().getField( np.right, true, false ).getIndex();
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

                            AlgRoot scanRoot = AlgRoot.of( originalProject, Kind.SELECT );
                            final PolyImplementation scanSig = prepareQuery( scanRoot, parameterRowType, false, false, true );
                            final ResultIterator iter = scanSig.execute( statement, -1 );
                            final List<List<PolyValue>> rows = iter.getAllRowsAndClose();

                            // Build new query tree
                            final List<ImmutableList<RexLiteral>> records = new ArrayList<>();
                            for ( final List<PolyValue> row : rows ) {
                                final List<RexLiteral> record = new ArrayList<>();
                                for ( int i = 0; i < row.size(); ++i ) {
                                    AlgDataType fieldType = originalProject.getTupleType().getFields().get( i ).getType();
                                    Pair<PolyValue, PolyType> converted = RexLiteral.convertType( row.get( i ), fieldType );
                                    record.add( new RexLiteral(
                                            converted.left,
                                            fieldType,
                                            converted.right
                                    ) );
                                }
                                records.add( ImmutableList.copyOf( record ) );
                            }
                            final ImmutableList<ImmutableList<RexLiteral>> values = ImmutableList.copyOf( records );
                            final AlgNode newValues = LogicalRelValues.create( originalProject.getCluster(), originalProject.getTupleType(), values );
                            final AlgNode newProject = LogicalRelProject.identity( newValues );

                            final AlgNode replacement = ltm.copy( ltm.getTraitSet(), Collections.singletonList( newProject ) );
                            // Schedule the index deletions
                            if ( !ltm.isInsert() ) {
                                for ( final Index index : indices ) {
                                    if ( ltm.isUpdate() ) {
                                        // Index not affected by this update, skip
                                        if ( index.getColumns().stream().noneMatch( ltm.getUpdateColumns()::contains ) ) {
                                            continue;
                                        }
                                    }
                                    final Set<Pair<List<PolyValue>, List<PolyValue>>> rowsToDelete = new HashSet<>( rows.size() );
                                    for ( List<PolyValue> row : rows ) {
                                        final List<PolyValue> rowProjection = new ArrayList<>( index.getColumns().size() );
                                        final List<PolyValue> targetRowProjection = new ArrayList<>( index.getTargetColumns().size() );
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
                                    if ( ltm.isUpdate() && index.getColumns().stream().noneMatch( ltm.getUpdateColumns()::contains ) ) {
                                        continue;
                                    }
                                    if ( ltm.isInsert() && index.getColumns().stream().noneMatch( ltm.getInput().getTupleType().getFieldNames()::contains ) ) {
                                        continue;
                                    }
                                    final Set<Pair<List<PolyValue>, List<PolyValue>>> rowsToReinsert = new HashSet<>( rows.size() );
                                    for ( List<PolyValue> row : rows ) {
                                        final List<PolyValue> rowProjection = new ArrayList<>( index.getColumns().size() );
                                        final List<PolyValue> targetRowProjection = new ArrayList<>( index.getTargetColumns().size() );
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
                    if ( node instanceof LogicalConditionalExecute lce ) {
                        final Index index = IndexManager.getInstance().getIndex(
                                lce.getLogicalNamespace(),
                                lce.getCatalogTable(),
                                lce.getCatalogColumns()
                        );
                        if ( index != null ) {
                            final LogicalConditionalExecute visited = (LogicalConditionalExecute) super.visit( lce );
                            Condition c = switch ( lce.getCondition() ) {
                                case TRUE, FALSE -> lce.getCondition();
                                case EQUAL_TO_ZERO -> index.containsAny( statement.getTransaction().getXid(), lce.getValues() ) ? Condition.FALSE : Condition.TRUE;
                                case GREATER_ZERO -> index.containsAny( statement.getTransaction().getXid(), lce.getValues() ) ? Condition.TRUE : Condition.FALSE;
                            };
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
            public AlgNode visit( LogicalRelProject project ) {
                if ( project.getInput() instanceof LogicalRelScan scan ) {
                    // Figure out the original column names required for index lookup
                    final List<String> columns = new ArrayList<>( project.getChildExps().size() );
                    final List<AlgDataType> ctypes = new ArrayList<>( project.getChildExps().size() );
                    for ( final RexNode expr : project.getChildExps() ) {
                        if ( !(expr instanceof RexIndexRef rir) ) {
                            IndexManager.getInstance().incrementMiss();
                            return super.visit( project );
                        }
                        final AlgDataTypeField field = scan.getTupleType().getFields().get( rir.getIndex() );
                        final String column = field.getName();
                        columns.add( column );
                        ctypes.add( field.getType() );
                    }
                    // Retrieve the catalog schema and database representations required for index lookup
                    final LogicalNamespace schema = statement.getTransaction().getDefaultNamespace();
                    final LogicalTable ctable = scan.getEntity().unwrap( LogicalTable.class ).orElseThrow();
                    // Retrieve any index and use for simplification
                    final Index idx = IndexManager.getInstance().getIndex( schema, ctable, columns );
                    if ( idx == null ) {
                        // No index available for simplification
                        IndexManager.getInstance().incrementNoIndex();
                        return super.visit( project );
                    }
                    // TODO: Avoid copying stuff around
                    final AlgDataType compositeType = builder.getTypeFactory().createStructType( null, ctypes, columns );
                    final Values replacement = idx.getAsValues( statement.getTransaction().getXid(), builder, compositeType );
                    final LogicalRelProject rProject = new LogicalRelProject(
                            replacement.getCluster(),
                            replacement.getTraitSet(),
                            replacement,
                            IntStream.range( 0, compositeType.getFieldCount() )
                                    .mapToObj( i -> rexBuilder.makeInputRef( replacement, i ) )
                                    .toList(),
                            compositeType );
                    IndexManager.getInstance().incrementHit();
                    return rProject;
                }
                return super.visit( project );
            }


            @Override
            public AlgNode visit( AlgNode node ) {
                if ( node instanceof LogicalRelProject lp ) {
                    lp.getMapping();
                }
                return super.visit( node );
            }

        };
        newRoot = newRoot.accept( shuttle2 );
        return AlgRoot.of( newRoot, logicalRoot.kind );
    }


    private List<ProposedRoutingPlan> route( AlgRoot logicalRoot, Statement statement, LogicalQueryInformation queryInformation ) {
        RoutingContext context = new RoutingContext( logicalRoot.alg.getCluster(), statement, queryInformation );
        final DmlRouter dmlRouter = RoutingManager.getInstance().getDmlRouter();
        if ( logicalRoot.getModel() == ModelTrait.GRAPH ) {
            return routeGraph( logicalRoot, queryInformation, dmlRouter );
        } else if ( logicalRoot.getModel() == ModelTrait.DOCUMENT ) {
            return routeDocument( logicalRoot, queryInformation, dmlRouter );
        } else if ( logicalRoot.alg instanceof LogicalRelModify ) {
            AlgNode routedDml = dmlRouter.routeDml( (LogicalRelModify) logicalRoot.alg, statement );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedDml, logicalRoot, queryInformation.getQueryHash() ) );
        } else if ( logicalRoot.alg instanceof ConditionalExecute ) {
            AlgNode routedConditionalExecute = dmlRouter.handleConditionalExecute( logicalRoot.alg, context );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedConditionalExecute, logicalRoot, queryInformation.getQueryHash() ) );
        } else if ( logicalRoot.alg instanceof BatchIterator ) {
            AlgNode routedIterator = dmlRouter.handleBatchIterator( logicalRoot.alg, context );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedIterator, logicalRoot, queryInformation.getQueryHash() ) );
        } else if ( logicalRoot.alg instanceof ConstraintEnforcer ) {
            AlgNode routedConstraintEnforcer = dmlRouter.handleConstraintEnforcer( logicalRoot.alg, context );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedConstraintEnforcer, logicalRoot, queryInformation.getQueryHash() ) );
        } else {
            final List<ProposedRoutingPlan> proposedPlans = new ArrayList<>();
            if ( statement.getTransaction().isAnalyze() ) {
                statement.getRoutingDuration().start( "Plan Proposing" );
            }

            for ( Router router : RoutingManager.getInstance().getRouters() ) {
                try {
                    List<RoutedAlgBuilder> builders = router.route( logicalRoot, context );
                    List<ProposedRoutingPlan> plans = builders.stream()
                            .map( builder -> (ProposedRoutingPlan) new ProposedRoutingPlanImpl( context, builder, logicalRoot, queryInformation.getQueryHash(), router.getClass() ) )
                            .toList();
                    proposedPlans.addAll( plans );
                } catch ( Throwable e ) {
                    log.warn( String.format( "Router: %s was not able to route the query.", router.getClass().getSimpleName() ) );
                }
            }

            if ( proposedPlans.isEmpty() ) {
                throw new GenericRuntimeException( "No router was able to route the query successfully." );
            }

            if ( statement.getTransaction().isAnalyze() ) {
                statement.getRoutingDuration().stop( "Plan Proposing" );
                statement.getRoutingDuration().start( "Remove Duplicates" );
            }

            final List<ProposedRoutingPlan> distinctPlans = proposedPlans.stream().distinct().toList();

            if ( distinctPlans.isEmpty() ) {
                throw new GenericRuntimeException( "No routing of query found" );
            }

            if ( statement.getTransaction().isAnalyze() ) {
                statement.getRoutingDuration().stop( "Remove Duplicates" );
            }

            return distinctPlans;
        }
    }


    private List<ProposedRoutingPlan> routeGraph( AlgRoot logicalRoot, LogicalQueryInformation queryInformation, DmlRouter dmlRouter ) {
        if ( logicalRoot.alg instanceof LogicalLpgModify ) {
            AlgNode routedDml = dmlRouter.routeGraphDml( (LogicalLpgModify) logicalRoot.alg, statement, null, null );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedDml, logicalRoot, queryInformation.getQueryHash() ) );
        }
        RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, logicalRoot.alg.getCluster() );
        AlgNode node = RoutingManager.getInstance().getRouters().get( 0 ).routeGraph( builder, (AlgNode & LpgAlg) logicalRoot.alg, statement );
        return Lists.newArrayList( new ProposedRoutingPlanImpl( builder.stackSize() == 0 ? node : builder.build(), logicalRoot, queryInformation.getQueryHash() ) );
    }


    private List<ProposedRoutingPlan> routeDocument( AlgRoot logicalRoot, LogicalQueryInformation queryInformation, DmlRouter dmlRouter ) {
        if ( logicalRoot.alg instanceof LogicalDocumentModify ) {
            AlgNode routedDml = dmlRouter.routeDocumentDml( (LogicalDocumentModify) logicalRoot.alg, statement, null, null );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedDml, logicalRoot, queryInformation.getQueryHash() ) );
        }
        RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, logicalRoot.alg.getCluster() );
        AlgNode node = RoutingManager.getInstance().getRouters().get( 0 ).routeDocument( builder, logicalRoot.alg, statement );
        return Lists.newArrayList( new ProposedRoutingPlanImpl( builder.stackSize() == 0 ? node : builder.build(), logicalRoot, queryInformation.getQueryHash() ) );
    }


    private List<Plan> routeCached( AlgRoot logicalRoot, List<CachedProposedRoutingPlan> routingPlansCached, RoutingContext context, boolean isAnalyze ) {
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
                context );

        if ( isAnalyze ) {
            statement.getRoutingDuration().stop( "Route Cached Plan" );
            statement.getRoutingDuration().start( "Create Plan From Cache" );
        }

        ProposedRoutingPlan proposed = new ProposedRoutingPlanImpl( context, builder, logicalRoot, context.getQueryInformation().getQueryHash(), selectedCachedPlan );

        if ( isAnalyze ) {
            statement.getRoutingDuration().stop( "Create Plan From Cache" );
        }

        return List.of( new Plan().proposedRoutingPlan( proposed ) );
    }


    private Pair<AlgRoot, AlgDataType> parameterize( AlgRoot routedRoot, AlgDataType parameterRowType ) {
        AlgNode routed = routedRoot.alg;
        List<AlgDataType> parameterRowTypeList = new ArrayList<>();
        parameterRowType.getFields().forEach( algDataTypeField -> parameterRowTypeList.add( algDataTypeField.getType() ) );

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
                    List<PolyValue> o = new ArrayList<>();
                    for ( ParameterValue v : values ) {
                        o.add( v.value() );
                    }
                    statement.getDataContext().addParameterValues( values.get( 0 ).index(), values.get( 0 ).type(), o );
                }

                statement.getDataContext().addContext();
            }
            statement.getDataContext().resetContext();
        } else {
            // Add values to data context
            for ( List<DataContext.ParameterValue> values : queryParameterizer.getValues().values() ) {
                List<PolyValue> o = new ArrayList<>();
                for ( ParameterValue v : values ) {
                    o.add( v.value() );
                }
                statement.getDataContext().addParameterValues( values.get( 0 ).index(), values.get( 0 ).type(), o );
            }
        }

        // parameterRowType
        AlgDataType newParameterRowType = statement.getTransaction().getTypeFactory().createStructType(
                types.stream().map( t -> 1L ).toList(),
                types,
                IntStream.range( 0, types.size() ).mapToObj( i -> "?" + i ).toList() );

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


    private PreparedResult<PolyValue> implement( AlgRoot root, AlgDataType parameterRowType ) {
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

        final Bindable<PolyValue[]> bindable;
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
                RexProgram program = RexProgram.create( enumerable.getTupleType(), projects, null, root.validatedRowType, rexBuilder );
                enumerable = EnumerableCalc.create( enumerable, program );
            }

            final Conformance conformance = statement.getPrepareContext().config().conformance();

            final Map<String, Object> internalParameters = new LinkedHashMap<>();
            internalParameters.put( "_conformance", conformance );

            Pair<Bindable<PolyValue[]>, String> implementationPair = EnumerableInterpretable.toBindable(
                    internalParameters,
                    enumerable,
                    prefer,
                    statement );
            bindable = implementationPair.left;
            generatedCode = implementationPair.right;
            statement.getDataContext().addAll( internalParameters );
        }

        AlgDataType resultType = root.alg.getTupleType();
        boolean isDml = root.kind.belongsTo( Kind.DML );

        return new PreparedResultImpl<>(
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
            public Bindable<PolyValue[]> getBindable( CursorFactory cursorFactory ) {
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


    private PolyImplementation createPolyImplementation( PreparedResult<PolyValue> preparedResult, Kind kind, AlgNode optimalNode, AlgDataType validatedRowType, Convention resultConvention, ExecutionTimeMonitor executionTimeMonitor, DataModel dataModel ) {
        final AlgDataType jdbcType = QueryProcessorHelpers.makeStruct( optimalNode.getCluster().getTypeFactory(), validatedRowType );
        return new PolyImplementation(
                jdbcType,
                dataModel,
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
                && (!algRoot.kind.belongsTo( Kind.DML ) || RuntimeConfig.QUERY_PLAN_CACHING_DML.getBoolean() || !statement.getDataContext().getParameterValues().isEmpty());
    }


    private boolean isImplementationCachingActive( Statement statement, AlgRoot algRoot ) {
        return RuntimeConfig.IMPLEMENTATION_CACHING.getBoolean()
                && statement.getTransaction().getUseCache()
                && (!algRoot.kind.belongsTo( Kind.DML ) || RuntimeConfig.IMPLEMENTATION_CACHING_DML.getBoolean() || !statement.getDataContext().getParameterValues().isEmpty());
    }


    private LogicalQueryInformation analyzeQueryAndPrepareMonitoring( Statement statement, AlgRoot logicalRoot, boolean isAnalyze, boolean isSubquery ) {
        // Analyze logical query
        LogicalAlgAnalyzeShuttle analyzer = new LogicalAlgAnalyzeShuttle( statement );
        logicalRoot.alg.accept( analyzer );

        // Get partitions of logical information
        Map<Long, Set<String>> partitionValueFilterPerScan = analyzer.getPartitionValueFilterPerScan();
        Map<Long, List<Long>> accessedPartitions = this.getAccessedPartitionsPerScan( logicalRoot.alg, partitionValueFilterPerScan );

        // Build queryClass from query-name and partitions.
        String queryHash = analyzer.getQueryName() + accessedPartitions;

        // Build LogicalQueryInformation instance and prepare monitoring
        LogicalQueryInformationImpl queryInformation = new LogicalQueryInformationImpl(
                queryHash,
                accessedPartitions,
                analyzer.availableColumns,
                analyzer.availableColumnsWithTable,
                analyzer.getUsedColumns(),
                analyzer.scannedEntities,
                analyzer.modifiedEntities );
        this.prepareMonitoring( statement, logicalRoot, isAnalyze, isSubquery, queryInformation );

        // Update which tables where changed used for Materialized Views
        MaterializedViewManager.getInstance().notifyModifiedEntities( statement.getTransaction(), queryInformation.allModifiedEntities );

        return queryInformation;
    }


    /**
     * Traverses all TablesScans used during execution and identifies for the corresponding table all
     * associated partitions that needs to be accessed, on the basis of the provided partitionValues identified in a LogicalFilter
     * <p>
     * It is necessary to associate the partitionIds again with the ScanId and not with the table itself. Because a table could be present
     * multiple times within one query. The aggregation per table would lead to data loss
     *
     * @param alg AlgNode to be processed
     * @param aggregatedPartitionValues Mapping of Scan Ids to identified partition Values
     * @return Mapping of Scan Ids to identified partition Ids
     */
    private Map<Long, List<Long>> getAccessedPartitionsPerScan( AlgNode alg, Map<Long, Set<String>> aggregatedPartitionValues ) {
        Map<Long, List<Long>> accessedPartitions = new HashMap<>(); // tableId  -> partitionIds
        if ( !(alg instanceof LogicalRelScan) ) {
            for ( int i = 0; i < alg.getInputs().size(); i++ ) {
                Map<Long, List<Long>> result = getAccessedPartitionsPerScan( alg.getInput( i ), aggregatedPartitionValues );
                if ( !result.isEmpty() ) {
                    for ( Map.Entry<Long, List<Long>> elem : result.entrySet() ) {
                        accessedPartitions.merge( elem.getKey(), elem.getValue(), ( l1, l2 ) -> Stream.concat( l1.stream(), l2.stream() ).toList() );
                    }
                }
            }
        } else {
            boolean fallback;
            if ( alg.getEntity() != null ) {
                Entity entity = alg.getEntity();

                if ( entity == null ) {
                    return accessedPartitions;
                }

                long scanId = entity.id;

                // Get placements of this table
                Optional<LogicalTable> optionalTable = entity.unwrap( LogicalTable.class );

                if ( optionalTable.isEmpty() ) {
                    return accessedPartitions;
                }
                LogicalTable table = optionalTable.get();

                PartitionProperty property = Catalog.snapshot().alloc().getPartitionProperty( table.id ).orElseThrow();
                fallback = true;

                if ( aggregatedPartitionValues.containsKey( scanId ) && aggregatedPartitionValues.get( scanId ) != null && !aggregatedPartitionValues.get( scanId ).isEmpty() ) {
                    fallback = false;
                    List<String> partitionValues = new ArrayList<>( aggregatedPartitionValues.get( scanId ) );

                    if ( log.isDebugEnabled() ) {
                        log.debug(
                                "TableID: {} is partitioned on column: {} - {}",
                                table.id,
                                property.partitionColumnId,
                                Catalog.getInstance().getSnapshot().rel().getColumn( property.partitionColumnId ).orElseThrow().name );

                    }
                    List<Long> identifiedPartitions = new ArrayList<>();
                    for ( String partitionValue : partitionValues ) {
                        if ( log.isDebugEnabled() ) {
                            log.debug( "Extracted PartitionValue: {}", partitionValue );
                        }
                        long identifiedPartition = PartitionManagerFactory.getInstance()
                                .getPartitionManager( property.partitionType )
                                .getTargetPartitionId( table, property, partitionValue );

                        identifiedPartitions.add( identifiedPartition );
                        if ( log.isDebugEnabled() ) {
                            log.debug( "Identified PartitionId: {} for value: {}", identifiedPartition, partitionValue );
                        }
                    }

                    accessedPartitions.merge(
                            scanId,
                            identifiedPartitions,
                            ( l1, l2 ) -> Stream.concat( l1.stream(), l2.stream() ).toList() );
                    scanPerTable.putIfAbsent( scanId, table.id );
                    // Fallback all partitionIds are needed
                }

                if ( fallback ) {
                    accessedPartitions.merge(
                            scanId,
                            property.partitionIds,
                            ( l1, l2 ) -> Stream.concat( l1.stream(), l2.stream() ).toList() );
                    scanPerTable.putIfAbsent( scanId, table.id );
                }

            }
        }
        return accessedPartitions;
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
            event.setMonitoringType( MonitoringType.from( logicalRoot.kind ) );
            statement.setMonitoringEvent( event );
        }
    }


    private void monitorResult( ProposedRoutingPlan selectedPlan ) {
        if ( statement.getMonitoringEvent() != null ) {
            StatementEvent eventData = statement.getMonitoringEvent();
            eventData.setAlgCompareString( selectedPlan.getRoutedRoot().alg.algCompareString() );
            if ( selectedPlan.getPhysicalQueryClass() != null ) {
                eventData.setPhysicalQueryClass( selectedPlan.getPhysicalQueryClass() );
                //eventData.setRowCount( (int) selectedPlan.getRoutedRoot().alg.estimateTupleCount( selectedPlan.getRoutedRoot().alg.getCluster().getMetadataQuery() ) );
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
     * <p>
     * Also remaps scanId to tableId to correctly update the accessed partition List
     *
     * @param eventData monitoring data to be updated
     */
    private void finalizeAccessedPartitions( StatementEvent eventData ) {
        Map<Long, List<Long>> partitionsInQueryInformation = eventData.getLogicalQueryInformation().getAccessedPartitions();
        Map<Long, Set<Long>> tempAccessedPartitions = new HashMap<>();

        for ( Entry<Long, List<Long>> entry : partitionsInQueryInformation.entrySet() ) {
            long scanId = entry.getKey();
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


    private Pair<PolyImplementation, ProposedRoutingPlan> selectPlan( ProposedImplementations proposed ) {
        List<ProposedRoutingPlan> proposedRoutingPlans = proposed.getRoutingPlans();
        LogicalQueryInformation queryInformation = proposed.getLogicalQueryInformation();

        List<AlgOptCost> approximatedCosts;
        if ( RuntimeConfig.ROUTING_PLAN_CACHING.getBoolean() ) {
            // Get approximated costs and cache routing plans
            approximatedCosts = proposed.plans.stream()
                    .map( p -> p.optimalNode().computeSelfCost( getPlanner(), p.optimalNode().getCluster().getMetadataQuery() ) )
                    .toList();
            this.cacheRouterPlans(
                    proposedRoutingPlans,
                    approximatedCosts,
                    queryInformation.getQueryHash(),
                    queryInformation.getAccessedPartitions().values().stream().flatMap( List::stream ).collect( Collectors.toSet() ) );
        }

        if ( proposed.plans.size() == 1 ) {
            // If only one plan proposed, return this without further selection
            if ( statement.getTransaction().isAnalyze() ) {
                UiRoutingPageUtil.outputSingleResult(
                        proposed.plans.get( 0 ),
                        statement.getTransaction().getQueryAnalyzer() );
                addGeneratedCodeToQueryAnalyzer( proposed.plans.get( 0 ).generatedCodes() );
            }
            return new Pair<>( proposed.plans.get( 0 ).result(), proposed.plans.get( 0 ).proposedRoutingPlan() );
        } else {
            // Calculate costs and get selected plan from plan selector
            approximatedCosts = proposed.plans.stream()
                    .map( p -> p.optimalNode().computeSelfCost( getPlanner(), p.optimalNode().getCluster().getMetadataQuery() ) )
                    .collect( Collectors.toList() );
            RoutingPlan routingPlan = RoutingManager.getInstance().getRoutingPlanSelector().selectPlanBasedOnCosts(
                    proposedRoutingPlans,
                    approximatedCosts,
                    statement );

            int index = proposedRoutingPlans.indexOf( (ProposedRoutingPlan) routingPlan );

            if ( statement.getTransaction().isAnalyze() ) {
                AlgNode optimalNode = proposed.plans.get( index ).optimalNode();
                UiRoutingPageUtil.addPhysicalPlanPage( optimalNode, statement.getTransaction().getQueryAnalyzer() );
                addGeneratedCodeToQueryAnalyzer( proposed.plans.get( index ).generatedCodes() );
            }

            return new Pair<>( proposed.plans.get( index ).result(), (ProposedRoutingPlan) routingPlan );
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

            // Clean Code (remove package names to make code better readable)
            String cleanedCode = code.replaceAll( "(org.)([a-z][a-z_0-9]*\\.)*", "" );

            Information informationCode = new InformationCode( group, cleanedCode );
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
            throw new GenericRuntimeException( "DeadLock while locking to reevaluate statistics", e );
        }
    }

}
