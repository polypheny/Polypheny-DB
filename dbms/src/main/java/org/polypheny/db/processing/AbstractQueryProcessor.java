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

package org.polypheny.db.processing;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataContext.ParameterValue;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.index.Index;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.PlanKey;
import org.polypheny.db.algebra.PlanKeys;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.AlgStructuredTypeFlattener;
import org.polypheny.db.algebra.constant.ConformanceEnum;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.ResultKind;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.common.ConditionalExecute.Condition;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlg.Prefer;
import org.polypheny.db.algebra.enumerable.EnumerableCalc;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.EnumerableInterpretable;
import org.polypheny.db.algebra.enumerable.EnumerableStreamer;
import org.polypheny.db.algebra.enumerable.common.EnumerableModifyCollect;
import org.polypheny.db.algebra.logical.common.LogicalBatchIterator;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.relational.LogicalModifyCollect;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.metadata.AlgColumnOrigin;
import org.polypheny.db.algebra.polyalg.PolyAlgMetadata.GlobalStats;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.entity.allocation.AllocationPlacement;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.functions.Functions;
import org.polypheny.db.information.InformationPolyAlg.PlanType;
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.interpreter.Interpreters;
import org.polypheny.db.monitoring.events.DmlEvent;
import org.polypheny.db.monitoring.events.MonitoringType;
import org.polypheny.db.monitoring.events.PlanFingerprint;
import org.polypheny.db.monitoring.events.QueryEvent;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgPlanner.CannotPlanException;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.volcano.VolcanoPlanner;
import org.polypheny.db.prepare.Prepare.PreparedResult;
import org.polypheny.db.prepare.Prepare.PreparedResultImpl;
import org.polypheny.db.processing.caching.ImplementationCache;
import org.polypheny.db.processing.caching.ImplementationCache.CachedImplementation;
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
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexSubQuery;
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
import org.polypheny.db.routing.routers.CachedPlanRouter;
import org.polypheny.db.routing.routers.SimpleRouter;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.PolyphenyDbException;
import org.polypheny.db.runtime.Typed;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.Program;
import org.polypheny.db.tools.Programs;
import org.polypheny.db.tools.RoutedAlgBuilder;
import org.polypheny.db.transaction.QueryExecutionScope;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.AlgEntityLockablesExtractor;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.transaction.locking.LockablesRegistry;
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


    protected AbstractQueryProcessor( Statement statement ) {
        this.statement = statement;
        if ( !statement.getTransaction().isActive() ) {
            throw new GenericRuntimeException( "Transaction is not active" );
        }
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
    public PolyImplementation prepareQuery( AlgRoot logicalRoot, boolean isRouted, boolean withMonitoring ) {
        return prepareQuery( logicalRoot, logicalRoot.alg.getCluster().getTypeFactory().builder().build(), isRouted, false, false, withMonitoring );
    }


    @Override
    public PolyImplementation prepareQuery( AlgRoot logicalRoot, boolean isRouted, boolean isPhysical, boolean withMonitoring ) {
        return prepareQuery( logicalRoot, logicalRoot.alg.getCluster().getTypeFactory().builder().build(), isRouted, isPhysical, false, withMonitoring );
    }


    @Override
    public PolyImplementation prepareQuery( AlgRoot logicalRoot, AlgDataType parameterRowType, boolean withMonitoring ) {
        return prepareQuery( logicalRoot, parameterRowType, false, false, withMonitoring );
    }


    @Override
    public PolyImplementation prepareQuery( AlgRoot logicalRoot, AlgDataType parameterRowType, boolean isRouted, boolean isSubquery, boolean withMonitoring ) {
        return prepareQuery( logicalRoot, parameterRowType, isRouted, false, isSubquery, withMonitoring );
    }


    @Override
    public PolyImplementation prepareQuery( AlgRoot root, AlgDataType parameterRowType, boolean isRouted, boolean isPhysical, boolean isSubquery, boolean withMonitoring ) {
        if ( !isSubquery && root.kind.belongsTo( Kind.DML ) ) {
            statement.setMaterializedDmlRowCount( null );
        }
        if ( isPhysical ) {
            return implementPhysicalPlan( root, parameterRowType );
        }
        if ( !isRouted && statement.isAnalyze() ) {
            attachPolyAlgPlan( root.alg );
        }

        if ( statement.isAnalyze() ) {
            statement.getOverviewDuration().start( "Processing" );
        }
        final ProposedImplementations proposedImplementations = prepareQueries( root, parameterRowType, isRouted, isSubquery );

        if ( statement.isAnalyze() ) {
            statement.getOverviewDuration().stop( "Processing" );
            statement.getOverviewDuration().start( "Plan Selection" );
        }

        final Pair<PolyImplementation, ProposedRoutingPlan> selectedPlan = selectPlan( proposedImplementations );

        if ( statement.isAnalyze() ) {
            statement.getOverviewDuration().stop( "Plan Selection" );
        }

        if ( withMonitoring ) {
            this.monitorResult( selectedPlan.right );
        }

        PolyImplementation implementation = selectedPlan.left;
        if ( !isSubquery && implementation.getDataModel() == DataModel.RELATIONAL
                && implementation.getResultKind() == ResultKind.ROWS && root.kind.belongsTo( Kind.QUERY ) ) {
            implementation.setIdentityFields( root.fields.stream().map( field -> isIdentityField( root, field.left ) ).toList() );
        }
        return implementation;
    }


    private boolean isIdentityField( AlgRoot root, int ordinal ) {
        AlgColumnOrigin origin = root.alg.getCluster().getMetadataQuery().getColumnOrigin( root.alg, ordinal );
        if ( origin == null || origin.isDerived() || !(origin.getOriginTable() instanceof LogicalTable table) ) {
            return false;
        }
        List<LogicalColumn> columns = root.alg.getCluster().getSnapshot().rel().getColumns( table.id );
        int sourceOrdinal = origin.getOriginColumnOrdinal();
        return sourceOrdinal >= 0 && sourceOrdinal < columns.size() && columns.get( sourceOrdinal ).getIdentity() != null;
    }


    private void attachPolyAlgPlan( AlgNode alg ) {
        ObjectMapper objectMapper = new ObjectMapper();
        GlobalStats gs = GlobalStats.computeGlobalStats( alg );
        try {
            ObjectNode objectNode = alg.serializePolyAlgebra( objectMapper, gs );
            String jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString( objectNode );
            String serialized = null;
            if ( shouldAttachTextualPolyAlg() ) {
                // when testing, we want to access the human-readable form
                serialized = alg.buildPolyAlgebra( (String) null );
                if ( serialized == null ) {
                    throw new GenericRuntimeException( "Could not serialize PolyAlgebra" );
                }
            }
            statement.getAnalyzer().registerQueryPlan( PlanType.LOGICAL, jsonString, serialized );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( e.getMessage(), e );
        }

    }


    private ProposedImplementations prepareQueries( AlgRoot logicalRoot, AlgDataType parameterRowType, boolean isRouted, boolean isSubQuery ) {
        boolean isAnalyze = statement.isAnalyze() && !isSubQuery;
        boolean lock = !isSubQuery;

        final Convention resultConvention = ENABLE_BINDABLE ? BindableConvention.INSTANCE : EnumerableConvention.INSTANCE;
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();

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
        lock &= !logicalQueryInformation.getAccessedPartitions().isEmpty();

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Analyze" );
        }

        ExecutionTimeMonitor executionTimeMonitor = new ExecutionTimeMonitor();
        if ( RoutingManager.POST_COST_AGGREGATION_ACTIVE.getBoolean() ) {
            // Subscribe only when aggregation is active
            executionTimeMonitor.subscribe( this, logicalQueryInformation.getQueryHash() );
        }

        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Parameter Validation" );
        }

        //
        // Validate parameter values
        ParameterValueValidator valueValidator = new ParameterValueValidator( logicalRoot.validatedRowType, statement.getDataContext() );
        valueValidator.visit( logicalRoot.alg );

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Parameter Validation" );
        }

        // Initialize result lists. They will all be with in the same ordering.
        List<Plan> plans;

        if ( isRouted ) {
            plans = List.of( new Plan().proposedRoutingPlan( new ProposedRoutingPlanImpl( logicalRoot, logicalQueryInformation.getQueryHash() ) ) );
        } else {
            // Locking
            if ( isAnalyze ) {
                statement.getProcessingDuration().start( "Locking" );
            }

            if ( lock ) {
                this.acquireLocks( logicalRoot );
            }

            // Index Update
            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Locking" );
                statement.getProcessingDuration().start( "Index Update" );
            }
            boolean usePolystoreIndexes = RuntimeConfig.POLYSTORE_INDEXES_ENABLED.getBoolean();
            if ( usePolystoreIndexes ) {
                IndexManager.getInstance().barrier( statement.getTransaction().getXid() );
            }
            boolean materializedUpdate = IdentityDmlMaterializer.usesIdentityUpdate( logicalRoot );
            boolean enforceConstraints = RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean() || RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean();
            boolean validateBeforeRouting = materializedUpdate && enforceConstraints
                    && RuntimeConfig.CONSTRAINT_ENFORCEMENT_STRATEGY.getEnum() == ConstraintStrategy.BEFORE_QUERY_EXECUTION;
            AlgRoot materializedRoot = IdentityDmlMaterializer.materialize( logicalRoot, statement, parameterRowType, validateBeforeRouting );
            boolean materializedWrite = materializedRoot != logicalRoot;
            logicalRoot = materializedRoot;
            AlgRoot indexUpdateRoot = logicalRoot;
            if ( usePolystoreIndexes && !materializedUpdate ) {
                indexUpdateRoot = indexUpdate( indexUpdateRoot, statement, parameterRowType );
            }

            //
            // Constraint Enforcement Rewrite
            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Index Update" );
                statement.getProcessingDuration().start( "Constraint Enforcement" );
            }
            AlgRoot constraintsRoot = indexUpdateRoot;

            if ( enforceConstraints && !validateBeforeRouting && constraintsRoot.kind.belongsTo( Kind.DML ) ) {
                // Check the final state of a SQL statement once all of its writes have finished.
                constraintsRoot = ConstraintEnforceAttacher.handleConstraints( constraintsRoot, statement );
            }

            //
            // Index Lookup Rewrite
            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Constraint Enforcement" );
                statement.getProcessingDuration().start( "Index Lookup Rewrite" );
            }

            AlgRoot indexLookupRoot = constraintsRoot;
            if ( usePolystoreIndexes && RuntimeConfig.POLYSTORE_INDEXES_SIMPLIFY.getBoolean() ) {
                indexLookupRoot = indexLookup( indexLookupRoot, statement );
            }
            if ( usePolystoreIndexes && materializedWrite ) {
                // Validate staged unique index entries before the physical write. Any
                // index-derived checks above still read the pre-statement contents.
                IndexManager.getInstance().barrier( statement.getTransaction().getXid() );
            }
            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Index Lookup Rewrite" );
                statement.getProcessingDuration().start( "Routing" );
            }

            //
            // Routing
            plans = route( indexLookupRoot, statement, logicalQueryInformation, isAnalyze ).stream()
                    .map( proposed -> new Plan().proposedRoutingPlan( proposed ) )
                    .toList();

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
                    && (RuntimeConfig.PARAMETERIZE_DML.getBoolean() || !routedRoot.kind.belongsTo( Kind.DML )
                    || (statement.getMaterializedDmlRowCount() != null && requiresFloatingLiteralBinding( routedRoot.alg ))) ) {
                Pair<AlgRoot, AlgDataType> parameterized = parameterize( routedRoot, parameterRowType );
                parameterizedRoot = parameterized.left;
            } else {
                // This query is an execution of a prepared statement
                parameterizedRoot = routedRoot;
            }

            plan.parameterizedRoot( parameterizedRoot );
            if ( isQueryPlanCachingActive( statement, routedRoot ) || isImplementationCachingActive( statement, routedRoot ) ) {
                plan.cacheKey( createCacheKey( parameterizedRoot, parameterRowType, resultConvention ) );
            }
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
                CachedImplementation cached = ImplementationCache.INSTANCE.getIfPresent( plan.cacheKey() );
                if ( cached != null ) {
                    AlgNode optimalNode = cached.optimalNode();
                    // Expose the same routed row type as a newly compiled implementation.
                    PolyImplementation result = createPolyImplementation(
                            cached.result(),
                            parameterizedRoot.kind,
                            optimalNode,
                            parameterizedRoot.alg.getTupleType(),
                            resultConvention,
                            executionTimeMonitor,
                            Objects.requireNonNull( optimalNode.getTraitSet().getTrait( ModelTraitDef.INSTANCE ) ).dataModel(),
                            parameterizedRoot.resultKind );
                    plan.result( result );
                    plan.generatedCodes( cached.result().getCode() );
                    plan.optimalNode( optimalNode );
                }
            }
        }

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Implementation Caching" );
        }

        // Can we return earlier?
        if ( plans.stream().allMatch( obj -> Objects.nonNull( obj.result() ) && Objects.nonNull( obj.optimalNode() ) ) ) {
            return new ProposedImplementations( plans.stream().filter( Plan::isValid ).toList() );
        }

        //
        // Plan Caching
        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Plan Caching" );
        }
        for ( Plan plan : plans ) {
            if ( plan.optimalNode() == null && plan.cacheKey() != null
                    && this.isQueryPlanCachingActive( statement, plan.proposedRoutingPlan().getRoutedRoot() ) ) {
                AlgNode cachedElem = QueryPlanCache.INSTANCE.getIfPresent( plan.cacheKey() );
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

            if ( plan.cacheKey() != null && this.isQueryPlanCachingActive( statement, plan.proposedRoutingPlan().getRoutedRoot() ) ) {
                QueryPlanCache.INSTANCE.put( plan.cacheKey(), plan.optimalNode() );
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
            AlgRoot optimalRoot = new AlgRoot( plan.optimalNode(), rowType, plan.parameterizedRoot().kind, fields, algCollation( plan.parameterizedRoot().alg ),
                    plan.parameterizedRoot().resultKind );

            PreparedResult<PolyValue> preparedResult = implement( optimalRoot, parameterRowType );

            // Cache implementation
            if ( this.isImplementationCachingActive( statement, plan.proposedRoutingPlan().getRoutedRoot() ) ) {
                if ( plan.cacheKey() != null && optimalRoot.alg.isImplementationCacheable() ) {
                    ImplementationCache.INSTANCE.put( plan.cacheKey(), new CachedImplementation( optimalRoot.alg, preparedResult ) );
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
                    Objects.requireNonNull( plan.optimalNode().getTraitSet().getTrait( ModelTraitDef.INSTANCE ) ).dataModel(),
                    optimalRoot.resultKind );
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
        return new ProposedImplementations( plans.stream().filter( Plan::isValid ).toList() );
    }


    Optional<List<List<PolyValue>>> readNativeInsertSource( LogicalRelModify insert, LogicalTable table, AlgDataType parameterRowType ) {
        if ( !(getPlanner() instanceof VolcanoPlanner planner) ) {
            return Optional.empty();
        }
        List<AlgDataTypeField> targetFields = table.getTupleType().getFields();
        if ( targetFields.stream().noneMatch( field -> PolyType.NUMERIC_TYPES.contains( field.getType().getPolyType() ) ) ) {
            return Optional.empty();
        }
        Snapshot snapshot = statement.getTransaction().getSnapshot();
        List<AllocationPlacement> placements = snapshot.alloc().getPlacementsFromLogical( table.id );
        if ( snapshot.alloc().getPartitionProperty( table.id ).orElseThrow().isPartitioned
                || placements.stream().anyMatch( placement -> snapshot.alloc().getColumns( placement.id ).size() != targetFields.size() ) ) {
            return Optional.empty();
        }
        List<DataStore<?>> stores = placements.stream()
                .map( placement -> AdapterManager.getInstance().getStore( placement.adapterId ) )
                .flatMap( Optional::stream )
                .filter( DataStore::supportsReadInsertSource )
                .toList();
        if ( stores.isEmpty() ) {
            return Optional.empty();
        }
        List<Long> keyIds = snapshot.rel().getPrimaryKey( table.primaryKey ).orElseThrow().fieldIds;
        Optional<Set<Integer>> volatileColumns = placements.size() > 1
                ? volatileInsertColumns( insert.getInput() ) : Optional.of( Set.of() );
        if ( volatileColumns.isEmpty() || targetFields.stream().anyMatch(
                field -> keyIds.contains( field.getId() ) && volatileColumns.get().contains( field.getIndex() ) ) ) {
            return Optional.empty();
        }
        List<Integer> comparedFields = targetFields.stream()
                .filter( field -> !volatileColumns.get().contains( field.getIndex() ) )
                .filter( field -> keyIds.contains( field.getId() ) || PolyType.NUMERIC_TYPES.contains( field.getType().getPolyType() ) )
                .map( AlgDataTypeField::getIndex )
                .toList();

        // Plan the ordinary DML without preparing or running its write, index or constraint actions.
        // The enclosing INSERT already holds the necessary locks on both source and destination.
        DataContext context = statement.getDataContext();
        List<Map<Long, PolyValue>> originalValues = context.getParameterValues();
        Map<Long, AlgDataType> originalTypes = Map.copyOf( context.getParameterTypes() );
        try {
            context.setParameterValues( new ArrayList<>( originalValues ) );
            AlgNode routed = RoutingManager.getInstance().getDmlRouter().routeRelationalDml( insert, statement );
            AlgRoot routedInsert = AlgRoot.of( routed, Kind.INSERT );
            if ( originalValues.isEmpty() && RuntimeConfig.PARAMETERIZE_DML.getBoolean() ) {
                Pair<AlgRoot, AlgDataType> parameters = parameterize( routedInsert, parameterRowType );
                routedInsert = parameters.left;
                parameterRowType = parameters.right;
            }
            AlgNode physicalInsert;
            try {
                physicalInsert = optimize( routedInsert, EnumerableConvention.INSTANCE );
            } catch ( CannotPlanException e ) {
                return Optional.empty();
            } finally {
                // The actual materialized INSERT has new row types and must not share
                // equivalence sets with the preview of its original source.
                planner.clearMemo();
                insert.getCluster().invalidateMetadataQuery();
            }
            List<AlgNode> writes = physicalInsert instanceof EnumerableModifyCollect ? physicalInsert.getInputs() : List.of( physicalInsert );
            List<InsertSourceRead> reads = new ArrayList<>();
            for ( AlgNode write : writes ) {
                EnumerableStreamer streamer = write instanceof EnumerableStreamer streamingWrite ? streamingWrite : null;
                AlgNode collector = streamer == null ? write : streamer.getCollector();
                for ( DataStore<?> store : stores ) {
                    Optional<AlgNode> source = store.readInsertSource( collector, targetFields );
                    if ( source.isPresent() ) {
                        Bindable<PolyValue[]> read = implement( AlgRoot.of( source.get(), Kind.SELECT ), parameterRowType ).getBindable();
                        reads.add( streamer == null ? new InsertSourceRead( read, null, List.of() )
                                : streamedInsertSource( read, streamer.getProvider(), parameterRowType ) );
                        break;
                    }
                }
            }
            if ( reads.isEmpty() ) {
                return Optional.empty();
            }

            List<Map<Long, PolyValue>> bindings = context.getParameterValues();
            List<List<PolyValue>> rows = new ArrayList<>();
            boolean consistent = true;
            for ( Map<Long, PolyValue> binding : bindings.isEmpty() ? List.<Map<Long, PolyValue>>of( Map.of() ) : bindings ) {
                context.setParameterValues( List.of( binding ) );
                List<List<PolyValue>> first = reads.get( 0 ).read( context );
                rows.addAll( first );
                if ( reads.size() > 1 ) {
                    Map<List<PolyValue>, Long> expected = nativeInsertRowCounts( first, comparedFields );
                    for ( InsertSourceRead read : reads.subList( 1, reads.size() ) ) {
                        consistent &= expected.equals( nativeInsertRowCounts( read.read( context ), comparedFields ) );
                    }
                }
            }
            if ( !consistent ) {
                throw new PolyphenyDbException( "Store placements disagree on the native INSERT source values.", null, "22000" );
            }
            // Remaining coordinator-driven writes receive the same complete, validated rows
            // through the normal materialized INSERT, including index and constraint checks.
            return Optional.of( rows );
        } finally {
            context.setParameterTypes( originalTypes );
            context.setParameterValues( originalValues );
        }
    }


    private static Optional<Set<Integer>> volatileInsertColumns( AlgNode source ) {
        if ( source instanceof LogicalRelProject project ) {
            Optional<Set<Integer>> input = volatileInsertColumns( project.getInput() );
            if ( input.isEmpty() ) {
                return Optional.empty();
            }
            Set<Integer> columns = new HashSet<>();
            for ( int i = 0; i < project.getProjects().size(); i++ ) {
                RexNode expression = project.getProjects().get( i );
                if ( isVolatileInsertExpression( expression ) || (!input.get().isEmpty()
                        && AlgOptUtil.InputFinder.bits( expression ).asList().stream().anyMatch( input.get()::contains )) ) {
                    columns.add( i );
                }
            }
            return Optional.of( columns );
        }
        if ( source instanceof LogicalRelUnion union && union.all ) {
            Set<Integer> columns = new HashSet<>();
            for ( AlgNode input : union.getInputs() ) {
                Optional<Set<Integer>> branch = volatileInsertColumns( input );
                if ( branch.isEmpty() ) {
                    return Optional.empty();
                }
                columns.addAll( branch.get() );
            }
            return Optional.of( columns );
        }

        // Preserve the existing single evaluation when volatility is outside a
        // projection or UNION ALL and may influence which rows the source returns.
        return hasVolatileInsertExpression( source ) ? Optional.empty() : Optional.of( Set.of() );
    }


    private static boolean hasVolatileInsertExpression( AlgNode source ) {
        return source.getChildExps().stream().anyMatch( AbstractQueryProcessor::isVolatileInsertExpression )
                || source.getInputs().stream().anyMatch( AbstractQueryProcessor::hasVolatileInsertExpression );
    }


    private static boolean isVolatileInsertExpression( RexNode expression ) {
        if ( expression instanceof RexSubQuery subQuery && hasVolatileInsertExpression( subQuery.alg ) ) {
            return true;
        }
        return expression instanceof RexCall call && (call.getOperator().isDynamicFunction() || !call.getOperator().isDeterministic()
                || call.getOperands().stream().anyMatch( AbstractQueryProcessor::isVolatileInsertExpression ));
    }


    private InsertSourceRead streamedInsertSource( Bindable<PolyValue[]> collector, AlgNode provider, AlgDataType parameterRowType ) {
        Bindable<PolyValue[]> read = implement( AlgRoot.of( provider, Kind.SELECT ), parameterRowType ).getBindable();
        List<AlgDataType> types = Functions.streamerParameterTypes(
                provider.getTupleType().getFields().stream().map( field -> field.getType().getPolyType() ).toList() );
        return new InsertSourceRead( collector, read, types );
    }


    private record InsertSourceRead( Bindable<PolyValue[]> collector, Bindable<PolyValue[]> provider, List<AlgDataType> streamedTypes ) {

        private List<List<PolyValue>> read( DataContext context ) {
            if ( provider == null ) {
                return readNativeInsertBinding( collector, context );
            }

            // The ordinary streamer first evaluates the complete provider, then binds
            // its output to the collector. Here the collector is a read-only projection.
            List<List<PolyValue>> providedRows = readNativeInsertBinding( provider, context );
            Map<Long, AlgDataType> types = context.getParameterTypes();
            List<Map<Long, PolyValue>> values = context.getParameterValues();
            try {
                List<List<PolyValue>> rows = new ArrayList<>();
                for ( List<PolyValue> row : providedRows ) {
                    QueryExecutionScope.remainingTimeoutMillis();
                    context.resetParameterValues();
                    for ( int i = 0; i < row.size(); i++ ) {
                        context.addParameterValues( i, streamedTypes.get( i ), Collections.singletonList( row.get( i ) ) );
                    }
                    rows.addAll( readNativeInsertBinding( collector, context ) );
                }
                return rows;
            } finally {
                context.setParameterTypes( types );
                context.setParameterValues( values );
            }
        }

    }


    private static List<List<PolyValue>> readNativeInsertBinding( Bindable<PolyValue[]> read, DataContext context ) {
        QueryExecutionScope.remainingTimeoutMillis();
        List<List<PolyValue>> rows = new ArrayList<>();
        try ( Enumerator<PolyValue[]> result = read.bind( context ).enumerator() ) {
            while ( result.moveNext() ) {
                QueryExecutionScope.remainingTimeoutMillis();
                rows.add( new ArrayList<>( Arrays.asList( result.current() ) ) );
            }
            return rows;
        } catch ( RuntimeException e ) {
            // The JDBC read wraps binding failures. Preserve the materialized
            // INSERT's numeric range error without changing ordinary JDBC binds.
            if ( ExceptionUtils.indexOfType( e, ArithmeticException.class ) >= 0
                    || ExceptionUtils.indexOfType( e, NumberFormatException.class ) >= 0 ) {
                throw new PolyphenyDbException( "Numeric assignment is out of range.", e, "22003" );
            }
            throw e;
        }
    }


    private static Map<List<PolyValue>, Long> nativeInsertRowCounts( List<List<PolyValue>> rows, List<Integer> fields ) {
        return rows.stream().collect( Collectors.groupingBy( row -> fields.stream().map( row::get ).toList(), Collectors.counting() ) );
    }


    private PolyImplementation implementPhysicalPlan( AlgRoot root, AlgDataType parameterRowType ) {
        final Convention resultConvention = ENABLE_BINDABLE ? BindableConvention.INSTANCE : EnumerableConvention.INSTANCE;
        ResultKind resultKind = root.resultKind == ResultKind.UPDATE_COUNT && isGraphInsertReturning( root )
                ? ResultKind.forGraphProjection( root.validatedRowType )
                : root.resultKind;

        PreparedResult<PolyValue> preparedResult = implement( root, parameterRowType );
        UiRoutingPageUtil.addPhysicalPlanPage( root.alg, statement.getAnalyzer(), shouldAttachTextualPolyAlg() );
        return createPolyImplementation(
                preparedResult,
                root.kind,
                root.alg,
                root.validatedRowType,
                resultConvention,
                new ExecutionTimeMonitor(),
                Objects.requireNonNull( root.alg.getTraitSet().getTrait( ModelTraitDef.INSTANCE ) ).dataModel(),
                resultKind );
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


    private void acquireLocks( AlgRoot logicalRoot ) {
        AlgEntityLockablesExtractor entityScanner = new AlgEntityLockablesExtractor();
        AlgOptUtil.go( entityScanner, logicalRoot.alg );
        Transaction transaction = statement.getTransaction();
        entityScanner.getResult().forEach( transaction::acquireLockable );
    }


    private AlgRoot indexUpdate( AlgRoot root, Statement statement, AlgDataType parameterRowType ) {
        if ( root.kind.belongsTo( Kind.DML ) ) {
            final AlgShuttle shuttle = new AlgShuttleImpl() {

                @Override
                public AlgNode visit( AlgNode node ) {
                    RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
                    if ( node instanceof LogicalRelModify ltm ) {
                        final Catalog catalog = Catalog.getInstance();
                        final LogicalTable table = ltm.getEntity().unwrapOrThrow( LogicalTable.class );
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
                                    if ( fieldValue instanceof RexLiteral rexLiteral ) {
                                        rowValues.add( rexLiteral.value );
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
                                    if ( fieldValue instanceof RexLiteral rexLiteral ) {
                                        targetRowValues.add( rexLiteral.value );
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
            return AlgRoot.of( newRoot, root.kind ).withResultKind( root.resultKind );

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
                    final LogicalTable ctable = scan.getEntity().unwrapOrThrow( LogicalTable.class );
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
        return AlgRoot.of( newRoot, logicalRoot.kind ).withResultKind( logicalRoot.resultKind );
    }


    private List<ProposedRoutingPlan> route( AlgRoot logicalRoot, Statement statement, LogicalQueryInformation queryInformation, boolean isAnalyze ) {
        RoutingContext context = new RoutingContext( logicalRoot.alg.getCluster(), statement, queryInformation );
        final DmlRouter dmlRouter = RoutingManager.getInstance().getDmlRouter();
        if ( logicalRoot.getModel() == ModelTrait.GRAPH ) {
            return routeGraph( logicalRoot, queryInformation, dmlRouter );
        } else if ( logicalRoot.getModel() == ModelTrait.DOCUMENT ) {
            return routeDocument( logicalRoot, queryInformation, dmlRouter );
        } else if ( logicalRoot.alg instanceof LogicalModifyCollect collect && logicalRoot.kind.belongsTo( Kind.DML ) ) {
            List<AlgNode> routedWrites = collect.getInputs().stream()
                    .map( write -> route( logicalRoot.withAlg( write ), statement, queryInformation, isAnalyze ).get( 0 ).getRoutedRoot().alg )
                    .toList();
            AlgNode routedDml = LogicalModifyCollect.create( routedWrites, collect.all );
            return List.of( new ProposedRoutingPlanImpl( routedDml, logicalRoot, queryInformation.getQueryHash() ) );
        } else if ( logicalRoot.alg instanceof LogicalRelModify logicalRelModify ) {
            AlgNode routedDml = dmlRouter.routeRelationalDml( logicalRelModify, statement );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedDml, logicalRoot, queryInformation.getQueryHash() ) );
        } else if ( logicalRoot.alg instanceof LogicalConditionalExecute logicalConditionalExecute ) {
            AlgNode routedConditionalExecute = dmlRouter.handleConditionalExecute( logicalConditionalExecute, context );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedConditionalExecute, logicalRoot, queryInformation.getQueryHash() ) );
        } else if ( logicalRoot.alg instanceof LogicalBatchIterator batchIterator ) {
            AlgNode routedIterator = dmlRouter.handleBatchIterator( batchIterator, context );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedIterator, logicalRoot, queryInformation.getQueryHash() ) );
        } else if ( logicalRoot.alg instanceof LogicalConstraintEnforcer logicalConstraintEnforcer ) {
            AlgNode routedConstraintEnforcer = dmlRouter.handleConstraintEnforcer( logicalConstraintEnforcer, context );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedConstraintEnforcer, logicalRoot, queryInformation.getQueryHash() ) );
        } else {
            List<Router> routers = RoutingManager.getInstance().getRouters();
            CachedPlanRouter cachedRouter = RoutingManager.getInstance().getCachedPlanRouter();
            Snapshot snapshot = Catalog.snapshot();
            PlanKey routingCacheKey = null;
            if ( RuntimeConfig.ROUTING_PLAN_CACHING.getBoolean()
                    && statement.getTransaction().getUseCache()
                    && routers.size() == 1 && routers.get( 0 ).getClass() == SimpleRouter.class
                    && cachedRouter.supports( logicalRoot, snapshot ) ) {
                routingCacheKey = RoutingPlanCache.keyFor( logicalRoot, queryInformation.getAccessedPartitions(), snapshot ).orElse( null );
                if ( routingCacheKey != null && snapshot == Catalog.snapshot() ) {
                    CachedProposedRoutingPlan cached = RoutingPlanCache.INSTANCE.getIfPresent( routingCacheKey );
                    if ( cached != null ) {
                        return List.of( routeCached( logicalRoot, cached, context, isAnalyze ) );
                    }
                }
            }

            final List<ProposedRoutingPlan> proposedPlans = new ArrayList<>();
            if ( statement.isAnalyze() ) {
                statement.getRoutingDuration().start( "Plan Proposing" );
            }

            for ( Router router : routers ) {
                try {
                    List<RoutedAlgBuilder> builders = router.route( logicalRoot, context );
                    List<ProposedRoutingPlan> plans = builders.stream()
                            .map( builder -> (ProposedRoutingPlan) new ProposedRoutingPlanImpl( context, builder, logicalRoot, queryInformation.getQueryHash(), router.getClass() ) )
                            .toList();
                    proposedPlans.addAll( plans );
                } catch ( Throwable e ) {
                    log.warn( "Router: {} was not able to route the query.", router.getClass().getSimpleName() );
                }
            }

            if ( proposedPlans.isEmpty() ) {
                throw new GenericRuntimeException( "No router was able to route the query successfully." );
            }

            if ( statement.isAnalyze() ) {
                statement.getRoutingDuration().stop( "Plan Proposing" );
                statement.getRoutingDuration().start( "Remove Duplicates" );
            }

            final List<ProposedRoutingPlan> distinctPlans = proposedPlans.stream().distinct().toList();

            if ( distinctPlans.isEmpty() ) {
                throw new GenericRuntimeException( "No routing of query found" );
            }

            if ( statement.isAnalyze() ) {
                statement.getRoutingDuration().stop( "Remove Duplicates" );
            }

            if ( routingCacheKey != null && snapshot == Catalog.snapshot() && context.getDirectScanAllocation() != null && distinctPlans.size() == 1 ) {
                RoutingPlanCache.INSTANCE.put( routingCacheKey,
                        new CachedProposedRoutingPlan( context.getDirectScanAllocation(), distinctPlans.get( 0 ).getPhysicalQueryClass() ) );
            }
            return distinctPlans;
        }
    }


    private static boolean isGraphInsertReturning( AlgRoot root ) {
        return root.kind == Kind.INSERT
                && root.getModel() == ModelTrait.GRAPH
                && root.alg instanceof LogicalConditionalExecute returning
                && returning.getLeft() instanceof LogicalLpgModify;
    }


    private List<ProposedRoutingPlan> routeGraph( AlgRoot logicalRoot, LogicalQueryInformation queryInformation, DmlRouter dmlRouter ) {
        if ( LogicalLpgModify.containsReturningWrite( logicalRoot.alg ) ) {
            // A later MATCH can read from a sibling of the returning write in the algebra tree.
            AlgNode routed = dmlRouter.routeGraphDml( logicalRoot.alg, statement );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routed, logicalRoot, queryInformation.getQueryHash() ) );
        }
        if ( isGraphInsertReturning( logicalRoot ) ) {
            LogicalConditionalExecute returning = (LogicalConditionalExecute) logicalRoot.alg;
            LogicalLpgModify insert = (LogicalLpgModify) returning.getLeft();
            AlgNode routedInsert = dmlRouter.routeGraphDml( insert, statement, null, null );
            AlgNode routed = LogicalConditionalExecute.create( routedInsert, returning.getRight(), returning );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routed, logicalRoot, queryInformation.getQueryHash() ) );
        }
        if ( logicalRoot.alg instanceof LogicalLpgModify logicalLpgModify ) {
            AlgNode routedDml = dmlRouter.routeGraphDml( logicalLpgModify, statement, null, null );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedDml, logicalRoot, queryInformation.getQueryHash() ) );
        }
        RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, logicalRoot.alg.getCluster() );
        AlgNode node = RoutingManager.getInstance().getRouters().get( 0 ).routeGraph( builder, (AlgNode & LpgAlg) logicalRoot.alg, statement );
        return Lists.newArrayList( new ProposedRoutingPlanImpl( builder.stackSize() == 0 ? node : builder.build(), logicalRoot, queryInformation.getQueryHash() ) );
    }


    private List<ProposedRoutingPlan> routeDocument( AlgRoot logicalRoot, LogicalQueryInformation queryInformation, DmlRouter dmlRouter ) {
        if ( logicalRoot.alg instanceof LogicalDocumentModify logicalDocumentModify ) {
            AlgNode routedDml = dmlRouter.routeDocumentDml( logicalDocumentModify, statement, null, null );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedDml, logicalRoot, queryInformation.getQueryHash() ) );
        }
        RoutedAlgBuilder builder = RoutedAlgBuilder.create( statement, logicalRoot.alg.getCluster() );
        AlgNode node = RoutingManager.getInstance().getRouters().get( 0 ).routeDocument( builder, logicalRoot.alg, statement );
        return Lists.newArrayList( new ProposedRoutingPlanImpl( builder.stackSize() == 0 ? node : builder.build(), logicalRoot, queryInformation.getQueryHash() ) );
    }


    private ProposedRoutingPlan routeCached( AlgRoot logicalRoot, CachedProposedRoutingPlan cached, RoutingContext context, boolean isAnalyze ) {
        if ( isAnalyze ) {
            statement.getRoutingDuration().start( "Route Cached Plan" );
        }

        RoutedAlgBuilder builder = RoutingManager.getInstance().getCachedPlanRouter().routeCached( logicalRoot, cached, context );
        ProposedRoutingPlanImpl plan = new ProposedRoutingPlanImpl( context, builder, logicalRoot, context.getQueryInformation().getQueryHash(), SimpleRouter.class );
        plan.setPhysicalQueryClass( cached.physicalQueryClass() );
        if ( isAnalyze ) {
            statement.getRoutingDuration().stop( "Route Cached Plan" );
        }
        return plan;
    }


    private static boolean requiresFloatingLiteralBinding( AlgNode node ) {
        if ( node instanceof LogicalRelValues values && values.getTuples().stream().flatMap( List::stream ).anyMatch( literal ->
                PolyType.APPROX_TYPES.contains( literal.getPolyType() ) && literal.getValue() != null && !literal.getValue().isNull()
                        && requiresFloatingLiteralBinding( literal.getValue().asNumber().doubleValue() ) ) ) {
            return true;
        }
        return node.getInputs().stream().anyMatch( AbstractQueryProcessor::requiresFloatingLiteralBinding );
    }


    private static boolean requiresFloatingLiteralBinding( double value ) {
        // Native SQL literal rendering goes through BigDecimal, which has neither non-finite values nor signed zero.
        return !Double.isFinite( value ) || Double.doubleToRawLongBits( value ) == Long.MIN_VALUE;
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
                routedRoot.withAlg( parameterized ),
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

            final Conformance conformance = ConformanceEnum.DEFAULT;

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
                Collections.nCopies( jdbcType.getFieldCount(), null ), // we do not have the origin of the fields so we provide null for all
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
            public Bindable<PolyValue[]> getBindable() {
                return bindable;
            }


            @Override
            public Type getElementType() {
                return ((Typed) bindable).getElementType();
            }
        };
    }


    private AlgCollation algCollation( AlgNode node ) {
        return node instanceof Sort sort
                ? sort.collation
                : AlgCollations.EMPTY;
    }


    private PolyImplementation createPolyImplementation( PreparedResult<PolyValue> preparedResult, Kind kind, AlgNode optimalNode, AlgDataType validatedRowType, Convention resultConvention, ExecutionTimeMonitor executionTimeMonitor, DataModel dataModel, ResultKind resultKind ) {
        final AlgDataType jdbcType = QueryProcessorHelpers.makeStruct( optimalNode.getCluster().getTypeFactory(), validatedRowType );
        return new PolyImplementation(
                jdbcType,
                dataModel,
                executionTimeMonitor,
                preparedResult,
                kind,
                statement,
                resultConvention,
                resultKind
        );
    }


    private PlanKey createCacheKey( AlgRoot root, AlgDataType parameterRowType, Convention resultConvention ) {
        Snapshot snapshot = root.alg.getCluster().getSnapshot();
        // Routing also reads the live catalog. Skip a plan built across a catalog change.
        if ( snapshot == null || snapshot != Catalog.snapshot() ) {
            return null;
        }
        return PlanKeys.forTree( root.alg )
                .flatMap( tree -> PlanKey.of( tree, root.kind, root.resultKind, root.validatedRowType, root.fields, root.collation,
                        parameterRowType, resultConvention.getName(), snapshot ) )
                .orElse( null );
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

        Map<Long, List<Long>> accessedPartitions = extractPartitions( logicalRoot.alg.getEntities() );

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


    private Map<Long, List<Long>> extractPartitions( Set<Entity> entities ) {
        Map<Long, List<Long>> map = new HashMap<>();
        for ( Entity entity : entities ) {
            if ( entity.isLogical() ) {
                map.computeIfAbsent( entity.getId(), k -> new ArrayList<>() ).addAll( Catalog.snapshot().alloc().getPartitionsFromLogical( entity.getId() ).stream().map( p -> p.id ).toList() );
            } else if ( entity.isAllocation() ) {
                map.computeIfAbsent( ((AllocationEntity) entity).getLogicalId(), k -> new ArrayList<>() ).add( ((AllocationEntity) entity).getPartitionId() );
            } else if ( entity.isPhysical() ) {
                map.computeIfAbsent( ((PhysicalEntity) entity).getLogicalId(), k -> new ArrayList<>() ).add( ((PhysicalEntity) entity).getAllocationId() );
            }
        }
        return map;
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
            if ( selectedPlan.getPhysicalQueryClass() != null ) {
                eventData.setPhysicalQueryClass( selectedPlan.getPhysicalQueryClass() );
                //eventData.setRowCount( (int) selectedPlan.getRoutedRoot().alg.estimateTupleCount( selectedPlan.getRoutedRoot().alg.getCluster().getMetadataQuery() ) );
            }

            if ( eventData instanceof QueryEvent queryEvent ) {
                queryEvent.setPlanFingerprint( PlanFingerprint.of( selectedPlan.getRoutedRoot().alg ) );
                if ( RoutingManager.POST_COST_AGGREGATION_ACTIVE.getBoolean() ) {
                    queryEvent.setUpdatePostCosts( true );
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
        // Otherwise, Analyzer cannot correctly analyze the event anymore
        if ( eventData.getAccessedPartitions() == null ) {
            eventData.setAccessedPartitions( Map.of() );
        }
    }


    private Pair<PolyImplementation, ProposedRoutingPlan> selectPlan( ProposedImplementations proposed ) {
        List<ProposedRoutingPlan> proposedRoutingPlans = proposed.getRoutingPlans();

        if ( proposed.plans.size() == 1 ) {
            // If only one plan proposed, return this without further selection
            if ( statement.isAnalyze() ) {
                UiRoutingPageUtil.outputSingleResult(
                        proposed.plans.get( 0 ),
                        statement.getAnalyzer(),
                        shouldAttachTextualPolyAlg() );
                addGeneratedCodeToQueryAnalyzer( proposed.plans.get( 0 ).generatedCodes() );
            }
            return new Pair<>( proposed.plans.get( 0 ).result(), proposed.plans.get( 0 ).proposedRoutingPlan() );
        } else {
            // Calculate costs and get selected plan from plan selector
            List<AlgOptCost> approximatedCosts = proposed.plans.stream()
                    .map( p -> p.optimalNode().computeSelfCost( getPlanner(), p.optimalNode().getCluster().getMetadataQuery() ) )
                    .collect( Collectors.toList() );
            RoutingPlan routingPlan = RoutingManager.getInstance().getRoutingPlanSelector().selectPlanBasedOnCosts(
                    proposedRoutingPlans,
                    approximatedCosts,
                    statement );

            int index = proposedRoutingPlans.indexOf( (ProposedRoutingPlan) routingPlan );

            if ( statement.isAnalyze() ) {
                AlgNode optimalNode = proposed.plans.get( index ).optimalNode();
                UiRoutingPageUtil.addPhysicalPlanPage( optimalNode, statement.getAnalyzer(), shouldAttachTextualPolyAlg() );
                addGeneratedCodeToQueryAnalyzer( proposed.plans.get( index ).generatedCodes() );
            }

            return new Pair<>( proposed.plans.get( index ).result(), (ProposedRoutingPlan) routingPlan );
        }
    }


    private void addGeneratedCodeToQueryAnalyzer( String code ) {
        if ( code != null ) {
            statement.getAnalyzer().registerImplementation( code );
        } else {
            log.error( "Generated code is null" );
        }
    }


    private boolean shouldAttachTextualPolyAlg() {
        return statement.getTransaction().getOrigin().equals( "PolyAlgParsingTest" );
    }


    /**
     * To acquire a global shared lock for a statement.
     * This method is used before the statistics are updated to make sure nothing changes during the updating process.
     */
    @Override
    public void lock( Transaction transaction ) {
        try {
            transaction.acquireLockable( LockablesRegistry.GLOBAL_SCHEMA_LOCKABLE, LockType.SHARED );
        } catch ( DeadlockException e ) {
            throw new GenericRuntimeException( "DeadLock while locking to reevaluate statistics", e );
        }
    }

}
