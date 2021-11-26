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

package org.polypheny.db.processing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataContext.ParameterValue;
import org.polypheny.db.adapter.enumerable.EnumerableCalc;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableInterpretable;
import org.polypheny.db.adapter.enumerable.EnumerableRel;
import org.polypheny.db.adapter.enumerable.EnumerableRel.Prefer;
import org.polypheny.db.adapter.index.Index;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.SchemaTypeVisitor;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.document.util.DataModelShuttle;
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.interpreter.Interpreters;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.DmlEvent;
import org.polypheny.db.monitoring.events.QueryEvent;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.partition.PartitionManagerFactory;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.plan.ViewExpanders;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.prepare.Prepare.PreparedResult;
import org.polypheny.db.prepare.Prepare.PreparedResultImpl;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.processing.caching.ImplementationCache;
import org.polypheny.db.processing.caching.QueryPlanCache;
import org.polypheny.db.processing.caching.RoutingPlanCache;
import org.polypheny.db.processing.shuttles.LogicalQueryInformationImpl;
import org.polypheny.db.processing.shuttles.ParameterValueValidator;
import org.polypheny.db.processing.shuttles.QueryParameterizer;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.RelShuttle;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.core.ConditionalExecute;
import org.polypheny.db.rel.core.ConditionalExecute.Condition;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.logical.LogicalConditionalExecute;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
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
import org.polypheny.db.routing.RoutingDebugUiPrinter;
import org.polypheny.db.routing.RoutingManager;
import org.polypheny.db.routing.RoutingPlan;
import org.polypheny.db.routing.dto.CachedProposedRoutingPlan;
import org.polypheny.db.routing.dto.ProposedRoutingPlanImpl;
import org.polypheny.db.routing.routers.CachedPlanRouter;
import org.polypheny.db.routing.strategies.RoutingPlanSelector;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.Typed;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.sql.SqlExplainFormat;
import org.polypheny.db.sql.SqlExplainLevel;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.validate.SqlConformance;
import org.polypheny.db.sql2rel.RelStructuredTypeFlattener;
import org.polypheny.db.tools.Program;
import org.polypheny.db.tools.Programs;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.tools.RoutedRelBuilder;
import org.polypheny.db.transaction.DeadlockException;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TableAccessMap;
import org.polypheny.db.transaction.TableAccessMap.Mode;
import org.polypheny.db.transaction.TableAccessMap.TableIdentifier;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.type.PolyType;
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
    protected static final boolean CONSTANT_REDUCTION = false;
    protected static final boolean ENABLE_STREAM = true;
    private final Statement statement;

    private final CachedPlanRouter cachedPlanRouter = RoutingManager.getInstance().getCachedPlanRouter();
    private final RoutingPlanSelector routingPlanSelector = RoutingManager.getInstance().getRoutingPlanSelector();
    private final RoutingDebugUiPrinter debugUiPrinter = RoutingManager.getInstance().getDebugUiPrinter();
    private final DmlRouter dmlRouter = RoutingManager.getInstance().getDmlRouter();

    private final Map<Integer, Long> scanPerTable = new HashMap<>(); // scanId  -> tableId //Needed for Lookup


    protected AbstractQueryProcessor( Statement statement ) {
        this.statement = statement;
    }


    @Override
    public void executionTime( String reference, long nanoTime ) {
        StatementEvent event = statement.getTransaction().getMonitoringEvent();
        if ( reference.equals( event.getLogicalQueryInformation().getQueryClass() ) ) {
            event.setExecutionTime( nanoTime );
        }
    }


    @Override
    public RelRoot expandView( RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath ) {
        return null; // TODO
    }


    @Override
    public void resetCaches() {
        ImplementationCache.INSTANCE.reset();
        QueryPlanCache.INSTANCE.reset();
        RoutingPlanCache.INSTANCE.reset();
        RoutingManager.getInstance().getRouters().forEach( Router::resetCaches );
    }


    @Override
    public PolyphenyDbSignature<?> prepareQuery( RelRoot logicalRoot, boolean withMonitoring ) {
        return prepareQuery( logicalRoot, logicalRoot.rel.getCluster().getTypeFactory().builder().build(), false, false, withMonitoring );
    }


    @Override
    public PolyphenyDbSignature<?> prepareQuery( RelRoot logicalRoot, RelDataType parameterRowType, boolean withMonitoring ) {
        return prepareQuery( logicalRoot, parameterRowType, false, false, withMonitoring );
    }


    @Override
    public PolyphenyDbSignature<?> prepareQuery( RelRoot logicalRoot, RelDataType parameterRowType, boolean isRouted, boolean isSubquery, boolean withMonitoring ) {
        final ProposedImplementations proposedImplementations = prepareQueryList( logicalRoot, parameterRowType, isRouted, isSubquery );

        if ( statement.getTransaction().isAnalyze() ) {
            statement.getProcessingDuration().start( "Plan Selection" );
        }

        final Pair<PolyphenyDbSignature<?>, ProposedRoutingPlan> executionResult = selectPlan( proposedImplementations );

        if ( statement.getTransaction().isAnalyze() ) {
            statement.getProcessingDuration().stop( "Plan Selection" );
        }

        if ( withMonitoring ) {
            this.monitorResult( executionResult.right );
        }

        return executionResult.left;
    }


    private ProposedImplementations prepareQueryList( RelRoot logicalRoot, RelDataType parameterRowType, boolean isRouted, boolean isSubQuery ) {
        boolean isAnalyze = statement.getTransaction().isAnalyze() && !isSubQuery;
        boolean lock = !isSubQuery;
        SchemaType schemaType = null;

        final Convention resultConvention = ENABLE_BINDABLE ? BindableConvention.INSTANCE : EnumerableConvention.INSTANCE;
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        // Initialize result lists. They will all be with in the same ordering.
        List<ProposedRoutingPlan> proposedRoutingPlans = null;
        List<Optional<RelNode>> optimalNodeList = new ArrayList<>();
        List<RelRoot> parameterizedRootList = new ArrayList<>();
        List<Optional<PolyphenyDbSignature<?>>> signatures = new ArrayList<>();

        // Check for view
        if ( logicalRoot.rel.hasView() ) {
            logicalRoot = logicalRoot.tryExpandView();
        }

        logicalRoot.rel.accept( new DataModelShuttle() );

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
            statement.getProcessingDuration().start( "Prepare Views" );
        }

        // Check if the relRoot includes Views or Materialized Views and replaces what necessary
        // View: replace LogicalViewTableScan with underlying information
        // Materialized View: add order by if Materialized View includes Order by
        ViewVisitor viewVisitor = new ViewVisitor( false );
        logicalRoot = viewVisitor.startSubstitution( logicalRoot );

        // Update which tables where changed used for Materialized Views
        TableUpdateVisitor visitor = new TableUpdateVisitor();
        logicalRoot.rel.accept( visitor );
        MaterializedViewManager.getInstance().addTables( statement.getTransaction(), visitor.getNames() );

        SchemaTypeVisitor schemaTypeVisitor = new SchemaTypeVisitor();
        logicalRoot.rel.accept( schemaTypeVisitor );
        schemaType = schemaTypeVisitor.getSchemaTypes();

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Prepare Views" );
        }

        if ( isRouted ) {
            proposedRoutingPlans = Lists.newArrayList( new ProposedRoutingPlanImpl( logicalRoot, logicalQueryInformation.getQueryClass() ) );
        } else {
            if ( lock ) {
                this.acquireLock( isAnalyze, logicalRoot );
            }

            // Index Update
            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Locking" );
                statement.getProcessingDuration().start( "Index Update" );
            }
            RelRoot indexUpdateRoot = logicalRoot;
            if ( RuntimeConfig.POLYSTORE_INDEXES_ENABLED.getBoolean() ) {
                IndexManager.getInstance().barrier( statement.getTransaction().getXid() );
                indexUpdateRoot = indexUpdate( indexUpdateRoot, statement, parameterRowType );
            }

            // Constraint Enforcement Rewrite
            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Index Update" );
                statement.getProcessingDuration().start( "Constraint Enforcement" );
            }
            RelRoot constraintsRoot = indexUpdateRoot;
            if ( RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean() || RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() ) {
                ConstraintEnforcer constraintEnforcer = new EnumerableConstraintEnforcer();
                constraintsRoot = constraintEnforcer.enforce( constraintsRoot, statement );
            }

            // Index Lookup Rewrite
            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Constraint Enforcement" );
                statement.getProcessingDuration().start( "Index Lookup Rewrite" );
            }
            RelRoot indexLookupRoot = constraintsRoot;
            if ( RuntimeConfig.POLYSTORE_INDEXES_ENABLED.getBoolean() && RuntimeConfig.POLYSTORE_INDEXES_SIMPLIFY.getBoolean() ) {
                indexLookupRoot = indexLookup( indexLookupRoot, statement );
            }

            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Index Lookup Rewrite" );
                statement.getProcessingDuration().start( "Routing" );
            }

            // Routing
            if ( RuntimeConfig.ROUTING_PLAN_CACHING.getBoolean() &&
                    !indexLookupRoot.kind.belongsTo( SqlKind.DML )
            ) {
                Set<Long> partitionIds = logicalQueryInformation.getAccessedPartitions().values().stream().flatMap( List::stream ).collect( Collectors.toSet() );
                List<CachedProposedRoutingPlan> routingPlansCached = RoutingPlanCache.INSTANCE.getIfPresent( logicalQueryInformation.getQueryClass(), partitionIds );
                if ( !routingPlansCached.isEmpty() ) {
                    proposedRoutingPlans = routeCached( indexLookupRoot, routingPlansCached, statement, logicalQueryInformation, isAnalyze );
                }
            }

            if ( proposedRoutingPlans == null ) {
                proposedRoutingPlans = route( indexLookupRoot, statement, logicalQueryInformation );
            }

            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Routing" );
                statement.getProcessingDuration().start( "Routing flattener" );
            }

            proposedRoutingPlans.forEach( proposedRoutingPlan -> {
                RelRoot routedRoot = proposedRoutingPlan.getRoutedRoot();
                RelStructuredTypeFlattener typeFlattener = new RelStructuredTypeFlattener(
                        RelBuilder.create( statement, routedRoot.rel.getCluster() ),
                        routedRoot.rel.getCluster().getRexBuilder(),
                        ViewExpanders.toRelContext( this, routedRoot.rel.getCluster() ),
                        true );
                proposedRoutingPlan.setRoutedRoot( routedRoot.withRel( typeFlattener.rewrite( routedRoot.rel ) ) );
            } );

            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Routing flattener" );
            }
        }

        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Parameter validation" );
        }

        // Validate parameterValues
        proposedRoutingPlans.forEach( proposedRoutingPlan ->
                new ParameterValueValidator( proposedRoutingPlan.getRoutedRoot().validatedRowType, statement.getDataContext() )
                        .visit( proposedRoutingPlan.getRoutedRoot().rel ) );

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Parameter validation" );
        }

        // Parameterize
        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Parameterize" );
        }

        // Add optional parameterizedRoots and signatures for all routed RelRoots.
        // Index of routedRoot, parameterizedRootList and signatures correspond!
        for ( ProposedRoutingPlan routingPlan : proposedRoutingPlans ) {
            RelRoot routedRoot = routingPlan.getRoutedRoot();
            RelRoot parameterizedRoot;
            if ( statement.getDataContext().getParameterValues().size() == 0 &&
                    (RuntimeConfig.PARAMETERIZE_DML.getBoolean() ||
                            !routedRoot.kind.belongsTo( SqlKind.DML ))
            ) {
                Pair<RelRoot, RelDataType> parameterized = parameterize( routedRoot, parameterRowType );
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

        // Implementation Caching
        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Implementation Caching" );
        }

        for ( int i = 0; i < proposedRoutingPlans.size(); i++ ) {
            RelRoot routedRoot = proposedRoutingPlans.get( i ).getRoutedRoot();
            if ( this.isImplementationCachingActive( statement, routedRoot ) ) {

                RelRoot parameterizedRoot = parameterizedRootList.get( i );
                PreparedResult preparedResult = ImplementationCache.INSTANCE.getIfPresent( parameterizedRoot.rel );
                if ( preparedResult != null ) {
                    PolyphenyDbSignature signature = createSignature( preparedResult, routedRoot, resultConvention, executionTimeMonitor );
                    signature.setSchemaType( schemaType );
                    signatures.add( Optional.of( signature ) );
                    optimalNodeList.add( Optional.of( routedRoot.rel ) );
                } else {
                    signatures.add( Optional.empty() );
                    optimalNodeList.add( Optional.empty() );
                }
            } else {
                signatures.add( Optional.empty() );
                optimalNodeList.add( Optional.empty() );
            }
        }

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Implementation Caching" );
        }

        // Can we return earlier?
        if ( signatures.stream().allMatch( Optional::isPresent ) && optimalNodeList.stream().allMatch( Optional::isPresent ) ) {
            return new ProposedImplementations(
                    proposedRoutingPlans,
                    optimalNodeList.stream().filter( Optional::isPresent ).map( Optional::get ).collect( Collectors.toList() ),
                    signatures.stream().filter( Optional::isPresent ).map( Optional::get ).collect( Collectors.toList() ),
                    logicalQueryInformation );
        }

        optimalNodeList = new ArrayList<>( Collections.nCopies( optimalNodeList.size(), Optional.empty() ) );

        // Plan Caching
        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Plan Caching" );
        }
        for ( int i = 0; i < proposedRoutingPlans.size(); i++ ) {
            if ( this.isQueryPlanCachingActive( statement, proposedRoutingPlans.get( i ).getRoutedRoot() ) ) {
                // Should always be the case
                RelNode cachedElem = QueryPlanCache.INSTANCE.getIfPresent( parameterizedRootList.get( i ).rel );
                if ( cachedElem != null ) {
                    optimalNodeList.set( i, Optional.of( cachedElem ) );
                }
            }
        }

        // Planning & Optimization
        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Plan Caching" );
            statement.getProcessingDuration().start( "Planning & Optimization" );
        }

        // OptimalNode same size as routed, parametrized and signature
        for ( int i = 0; i < optimalNodeList.size(); i++ ) {
            if ( optimalNodeList.get( i ).isPresent() ) {
                continue;
            }
            RelRoot parameterizedRoot = parameterizedRootList.get( i );
            RelRoot routedRoot = proposedRoutingPlans.get( i ).getRoutedRoot();
            optimalNodeList.set( i, Optional.of( optimize( parameterizedRoot, resultConvention ) ) );

            if ( this.isQueryPlanCachingActive( statement, routedRoot ) ) {
                QueryPlanCache.INSTANCE.put( parameterizedRoot.rel, optimalNodeList.get( i ).get() );
            }
        }

        // Implementation
        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Planning & Optimization" );
            statement.getProcessingDuration().start( "Implementation" );
        }

        for ( int i = 0; i < optimalNodeList.size(); i++ ) {
            if ( signatures.get( i ).isPresent() ) {
                continue;
            }

            RelNode optimalNode = optimalNodeList.get( i ).get();
            RelRoot parameterizedRoot = parameterizedRootList.get( i );
            RelRoot routedRoot = proposedRoutingPlans.get( i ).getRoutedRoot();

            final RelDataType rowType = parameterizedRoot.rel.getRowType();
            final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
            RelRoot optimalRoot = new RelRoot( optimalNode, rowType, parameterizedRoot.kind, fields, relCollation( parameterizedRoot.rel ) );

            PreparedResult preparedResult = implement( optimalRoot, parameterRowType );

            // Cache implementation
            if ( this.isImplementationCachingActive( statement, routedRoot ) ) {
                if ( optimalRoot.rel.isImplementationCacheable() ) {
                    ImplementationCache.INSTANCE.put( parameterizedRoot.rel, preparedResult );
                } else {
                    ImplementationCache.INSTANCE.countUncacheable();
                }
            }

            PolyphenyDbSignature<?> signature = createSignature( preparedResult, optimalRoot, resultConvention, executionTimeMonitor );
            signature.setSchemaType( schemaType );
            signatures.set( i, Optional.of( signature ) );
            optimalNodeList.set( i, Optional.of( optimalRoot.rel ) );
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
                optimalNodeList.stream().filter( Optional::isPresent ).map( Optional::get ).collect( Collectors.toList() ),
                signatures.stream().filter( Optional::isPresent ).map( Optional::get ).collect( Collectors.toList() ),
                logicalQueryInformation );
    }


    @AllArgsConstructor
    @Getter
    private static class ProposedImplementations {

        private final List<ProposedRoutingPlan> proposedRoutingPlans;
        private final List<RelNode> optimizedPlans;
        private final List<PolyphenyDbSignature<?>> signatures;
        private final LogicalQueryInformation logicalQueryInformation;

    }


    private void acquireLock( boolean isAnalyze, RelRoot logicalRoot ) {

        // Locking
        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Locking" );
        }
        try {
            // Get a shared global schema lock (only DDLs acquire a exclusive global schema lock)
            LockManager.INSTANCE.lock( LockManager.GLOBAL_LOCK, (TransactionImpl) statement.getTransaction(), LockMode.SHARED );
            // Get locks for individual tables
            TableAccessMap accessMap = new TableAccessMap( logicalRoot.rel );
            for ( TableIdentifier tableIdentifier : accessMap.getTablesAccessed() ) {
                Mode mode = accessMap.getTableAccessMode( tableIdentifier );
                if ( mode == Mode.READ_ACCESS ) {
                    LockManager.INSTANCE.lock( tableIdentifier, (TransactionImpl) statement.getTransaction(), LockMode.SHARED );
                } else if ( mode == Mode.WRITE_ACCESS || mode == Mode.READWRITE_ACCESS ) {
                    LockManager.INSTANCE.lock( tableIdentifier, (TransactionImpl) statement.getTransaction(), LockMode.EXCLUSIVE );
                }
            }
        } catch ( DeadlockException e ) {
            throw new RuntimeException( e );
        }
    }


    private RelRoot indexUpdate( RelRoot root, Statement statement, RelDataType parameterRowType ) {
        if ( root.kind.belongsTo( SqlKind.DML ) ) {
            final RelShuttle shuttle = new RelShuttleImpl() {

                @Override
                public RelNode visit( RelNode node ) {
                    RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
                    if ( node instanceof LogicalTableModify ) {
                        final Catalog catalog = Catalog.getInstance();
                        final LogicalTableModify ltm = (LogicalTableModify) node;
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
                            final LogicalValues lvalues = (LogicalValues) ltm.getInput( 0 ).accept( new RelDeepCopyShuttle() );
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
                            final LogicalProject lproject = (LogicalProject) ltm.getInput().accept( new RelDeepCopyShuttle() );
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
                            RelNode original = ltm.getInput().accept( new RelDeepCopyShuttle() );
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
                            RelRoot scanRoot = RelRoot.of( originalProject, SqlKind.SELECT );
                            final PolyphenyDbSignature scanSig = prepareQuery( scanRoot, parameterRowType, false, false, true );
                            final Iterable<Object> enumerable = scanSig.enumerable( statement.getDataContext() );
                            final Iterator<Object> iterator = enumerable.iterator();
                            final List<List<Object>> rows = MetaImpl.collect( scanSig.cursorFactory, iterator, new ArrayList<>() );
                            // Build new query tree
                            final List<ImmutableList<RexLiteral>> records = new ArrayList<>();
                            for ( final List<Object> row : rows ) {
                                final List<RexLiteral> record = new ArrayList<>();
                                for ( int i = 0; i < row.size(); ++i ) {
                                    RelDataType fieldType = originalProject.getRowType().getFieldList().get( i ).getType();
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
                            final RelNode newValues = LogicalValues.create( originalProject.getCluster(), originalProject.getRowType(), values );
                            final RelNode newProject = LogicalProject.identity( newValues );
//                            List<RexNode> sourceExpr = ltm.getSourceExpressionList();
//                            if ( ltm.isUpdate() || ltm.isMerge() ) {
//                                //FIXME(s3lph): Wrong index
//                                sourceExpr = IntStream.range( 0, sourceExpr.size() )
//                                        .mapToObj( i -> rexBuilder.makeFieldAccess( rexBuilder.makeInputRef( newProject, i ), 0 ) )
//                                        .collect( Collectors.toList() );
//                            }
//                            final RelNode replacement = LogicalTableModify.create(
//                                    ltm.getTable(),
//                                    transaction.getCatalogReader(),
//                                    newProject,
//                                    ltm.getOperation(),
//                                    ltm.getUpdateColumnList(),
//                                    sourceExpr,
//                                    ltm.isFlattened()
//                            );
                            final RelNode replacement = ltm.copy( ltm.getTraitSet(), Collections.singletonList( newProject ) );
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
            final RelNode newRoot = shuttle.visit( root.rel );
            return RelRoot.of( newRoot, root.kind );

        }
        return root;
    }


    private RelRoot indexLookup( RelRoot logicalRoot, Statement statement ) {
        final RelBuilder builder = RelBuilder.create( statement, logicalRoot.rel.getCluster() );
        final RexBuilder rexBuilder = builder.getRexBuilder();
        RelNode newRoot = logicalRoot.rel;
        if ( logicalRoot.kind.belongsTo( SqlKind.DML ) ) {
            final RelShuttle shuttle = new RelShuttleImpl() {

                @Override
                public RelNode visit( RelNode node ) {
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
        final RelShuttle shuttle2 = new RelShuttleImpl() {

            @Override
            public RelNode visit( LogicalProject project ) {
                if ( project.getInput() instanceof LogicalTableScan ) {
                    // Figure out the original column names required for index lookup
                    final LogicalTableScan scan = (LogicalTableScan) project.getInput();
                    final String table = scan.getTable().getQualifiedName().get( scan.getTable().getQualifiedName().size() - 1 );
                    final List<String> columns = new ArrayList<>( project.getChildExps().size() );
                    final List<RelDataType> ctypes = new ArrayList<>( project.getChildExps().size() );
                    for ( final RexNode expr : project.getChildExps() ) {
                        if ( !(expr instanceof RexInputRef) ) {
                            IndexManager.getInstance().incrementMiss();
                            return super.visit( project );
                        }
                        final RexInputRef rir = (RexInputRef) expr;
                        final RelDataTypeField field = scan.getRowType().getFieldList().get( rir.getIndex() );
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
                    final RelDataType compositeType = builder.getTypeFactory().createStructType( ctypes, columns );
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
            public RelNode visit( RelNode node ) {
                if ( node instanceof LogicalProject ) {
                    final LogicalProject lp = (LogicalProject) node;
                    lp.getMapping();
                }
                return super.visit( node );
            }

        };
        newRoot = newRoot.accept( shuttle2 );
        return RelRoot.of( newRoot, logicalRoot.kind );
    }


    private List<ProposedRoutingPlan> route( RelRoot logicalRoot, Statement statement, LogicalQueryInformation queryInformation ) {
        if ( logicalRoot.rel instanceof LogicalTableModify ) {
            RelNode routedDml = dmlRouter.routeDml( logicalRoot.rel, statement );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedDml, logicalRoot, queryInformation.getQueryClass() ) );
        } else if ( logicalRoot.rel instanceof ConditionalExecute ) {
            RelNode routedConditionalExecute = dmlRouter.handleConditionalExecute( logicalRoot.rel, statement, queryInformation );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedConditionalExecute, logicalRoot, queryInformation.getQueryClass() ) );
        } else {
            final List<ProposedRoutingPlan> proposedPlans = new ArrayList<>();
            if ( statement.getTransaction().isAnalyze() ) {
                statement.getProcessingDuration().start( "Routing Plan Proposing" );
            }

            for ( Router router : RoutingManager.getInstance().getRouters() ) {
                List<RoutedRelBuilder> builders = router.route( logicalRoot, statement, queryInformation );
                List<ProposedRoutingPlan> plans = builders.stream()
                        .map( builder -> new ProposedRoutingPlanImpl( builder, logicalRoot, queryInformation.getQueryClass(), router.getClass() ) )
                        .collect( Collectors.toList() );
                proposedPlans.addAll( plans );
            }

            if ( statement.getTransaction().isAnalyze() ) {
                statement.getProcessingDuration().stop( "Routing Plan Proposing" );
                statement.getProcessingDuration().start( "Routing Plan Remove Duplicates" );
            }

            final List<ProposedRoutingPlan> distinctPlans = proposedPlans.stream().distinct().collect( Collectors.toList() );

            if ( distinctPlans.isEmpty() ) {
                throw new RuntimeException( "No routing of query found" );
            }

            if ( statement.getTransaction().isAnalyze() ) {
                statement.getProcessingDuration().stop( "Routing Plan Remove Duplicates" );
            }

            return distinctPlans;
        }
    }


    private List<ProposedRoutingPlan> routeCached( RelRoot logicalRoot, List<CachedProposedRoutingPlan> routingPlansCached, Statement statement, LogicalQueryInformation queryInformation, boolean isAnalyze ) {
        // TODO: get only best plan.

        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Plan Selection" );
        }

        CachedProposedRoutingPlan selectedCachedPlan = selectCachedPlan( routingPlansCached, queryInformation.getQueryClass() );

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Plan Selection" );
        }

        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Route Cached" );
        }

        RoutedRelBuilder builder = cachedPlanRouter.routeCached( logicalRoot, selectedCachedPlan, statement );

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Route Cached" );
            statement.getProcessingDuration().start( "Create Plan From Cache" );
        }

        ProposedRoutingPlan proposed = new ProposedRoutingPlanImpl( builder, logicalRoot, queryInformation.getQueryClass(), selectedCachedPlan );

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Create Plan From Cache" );
        }

        return Lists.newArrayList( proposed );
    }


    private Pair<RelRoot, RelDataType> parameterize( RelRoot routedRoot, RelDataType parameterRowType ) {
        RelNode routed = routedRoot.rel;
        List<RelDataType> parameterRowTypeList = new ArrayList<>();
        parameterRowType.getFieldList().forEach( relDataTypeField -> parameterRowTypeList.add( relDataTypeField.getType() ) );

        // Parameterize
        QueryParameterizer queryParameterizer = new QueryParameterizer( parameterRowType.getFieldCount(), parameterRowTypeList );
        RelNode parameterized = routed.accept( queryParameterizer );
        List<RelDataType> types = queryParameterizer.getTypes();

        // Add values to data context
        for ( List<DataContext.ParameterValue> values : queryParameterizer.getValues().values() ) {
            List<Object> o = new ArrayList<>();
            for ( ParameterValue v : values ) {
                o.add( v.getValue() );
            }
            statement.getDataContext().addParameterValues( values.get( 0 ).getIndex(), values.get( 0 ).getType(), o );
        }

        // parameterRowType
        RelDataType newParameterRowType = statement.getTransaction().getTypeFactory().createStructType(
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
                new RelRoot( parameterized, routedRoot.validatedRowType, routedRoot.kind, routedRoot.fields, routedRoot.collation ),
                newParameterRowType
        );
    }


    private RelNode optimize( RelRoot logicalRoot, Convention resultConvention ) {
        RelNode logicalPlan = logicalRoot.rel;

        final RelTraitSet desiredTraits = logicalPlan.getTraitSet()
                .replace( resultConvention )
                .replace( relCollation( logicalPlan ) )
                .simplify();

        final Program program = Programs.standard();
        final RelNode rootRel4 = program.run( getPlanner(), logicalPlan, desiredTraits );

        //final RelNode relNode = getPlanner().changeTraits( root.rel, desiredTraits );
        //getPlanner().setRoot(relNode);
        //final RelNode rootRel4 = getPlanner().findBestExp();

        return rootRel4;
    }


    private PreparedResult implement( RelRoot root, RelDataType parameterRowType ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "Physical query plan: [{}]", RelOptUtil.dumpPlan( "-- Physical Plan", root.rel, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }

        final RelDataType jdbcType = QueryProcessorHelpers.makeStruct( root.rel.getCluster().getTypeFactory(), root.validatedRowType );
        List<List<String>> fieldOrigins = Collections.nCopies( jdbcType.getFieldCount(), null );

        final Prefer prefer = Prefer.ARRAY;
        final Convention resultConvention =
                ENABLE_BINDABLE
                        ? BindableConvention.INSTANCE
                        : EnumerableConvention.INSTANCE;

        final Bindable bindable;
        if ( resultConvention == BindableConvention.INSTANCE ) {
            bindable = Interpreters.bindable( root.rel );
        } else {
            EnumerableRel enumerable = (EnumerableRel) root.rel;
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
                final SqlConformance conformance = statement.getPrepareContext().config().conformance();

                final Map<String, Object> internalParameters = new LinkedHashMap<>();
                internalParameters.put( "_conformance", conformance );

                bindable = EnumerableInterpretable.toBindable( internalParameters, statement.getPrepareContext().spark(), enumerable, prefer, statement );
                statement.getDataContext().addAll( internalParameters );
            } finally {
                CatalogReader.THREAD_LOCAL.remove();
            }
        }

        RelDataType resultType = root.rel.getRowType();
        boolean isDml = root.kind.belongsTo( SqlKind.DML );

        return new PreparedResultImpl(
                resultType,
                parameterRowType,
                fieldOrigins,
                root.collation.getFieldCollations().isEmpty()
                        ? ImmutableList.of()
                        : ImmutableList.of( root.collation ),
                root.rel,
                QueryProcessorHelpers.mapTableModOp( isDml, root.kind ),
                isDml ) {
            @Override
            public String getCode() {
                throw new UnsupportedOperationException();
            }


            @Override
            public Bindable getBindable( CursorFactory cursorFactory ) {
                return bindable;
            }


            @Override
            public Type getElementType() {
                return ((Typed) bindable).getElementType();
            }
        };
    }


    private RelCollation relCollation( RelNode node ) {
        return node instanceof Sort
                ? ((Sort) node).collation
                : RelCollations.EMPTY;
    }


    private PolyphenyDbSignature createSignature( PreparedResult preparedResult, RelRoot optimalRoot, Convention resultConvention, ExecutionTimeMonitor executionTimeMonitor ) {
        final RelDataType jdbcType = QueryProcessorHelpers.makeStruct( optimalRoot.rel.getCluster().getTypeFactory(), optimalRoot.validatedRowType );
        final List<AvaticaParameter> parameters = new ArrayList<>();
        for ( RelDataTypeField field : preparedResult.getParameterRowType().getFieldList() ) {
            RelDataType type = field.getType();
            parameters.add(
                    new AvaticaParameter(
                            false,
                            QueryProcessorHelpers.getPrecision( type ),
                            0, // This is a workaround for a bug in Avatica with Decimals. There is no need to change the scale //getScale( type ),
                            QueryProcessorHelpers.getTypeOrdinal( type ),
                            type.getPolyType().getTypeName(),
                            type.getClass().getName(),
                            field.getName() ) );
        }

        final RelDataType x;
        switch ( optimalRoot.kind ) {
            case INSERT:
            case DELETE:
            case UPDATE:
            case EXPLAIN:
                // FIXME: getValidatedNodeType is wrong for DML
                x = RelOptUtil.createDmlRowType( optimalRoot.kind, statement.getTransaction().getTypeFactory() );
                break;
            default:
                x = optimalRoot.validatedRowType;
        }
        final List<ColumnMetaData> columns = QueryProcessorHelpers.getColumnMetaDataList(
                statement.getTransaction().getTypeFactory(),
                x,
                QueryProcessorHelpers.makeStruct( statement.getTransaction().getTypeFactory(), x ),
                preparedResult.getFieldOrigins() );
        Class resultClazz = null;
        if ( preparedResult instanceof Typed ) {
            resultClazz = (Class) ((Typed) preparedResult).getElementType();
        }
        final CursorFactory cursorFactory =
                resultConvention == BindableConvention.INSTANCE
                        ? CursorFactory.ARRAY
                        : CursorFactory.deduce( columns, resultClazz );
        final Bindable bindable = preparedResult.getBindable( cursorFactory );

        return new PolyphenyDbSignature<Object[]>(
                "",
                parameters,
                ImmutableMap.of(),
                jdbcType,
                columns,
                cursorFactory,
                statement.getTransaction().getSchema(),
                ImmutableList.of(),
                -1,
                bindable,
                QueryProcessorHelpers.getStatementType( preparedResult ),
                executionTimeMonitor );
    }


    private boolean isQueryPlanCachingActive( Statement statement, RelRoot relRoot ) {
        return RuntimeConfig.QUERY_PLAN_CACHING.getBoolean()
                && statement.getTransaction().getUseCache()
                && (!relRoot.kind.belongsTo( SqlKind.DML ) || RuntimeConfig.QUERY_PLAN_CACHING_DML.getBoolean() || statement.getDataContext().getParameterValues().size() > 0);
    }


    private boolean isImplementationCachingActive( Statement statement, RelRoot relRoot ) {
        return RuntimeConfig.IMPLEMENTATION_CACHING.getBoolean()
                && statement.getTransaction().getUseCache()
                && (!relRoot.kind.belongsTo( SqlKind.DML ) || RuntimeConfig.IMPLEMENTATION_CACHING_DML.getBoolean() || statement.getDataContext().getParameterValues().size() > 0);
    }


    private LogicalQueryInformation analyzeQueryAndPrepareMonitoring( Statement statement, RelRoot logicalRoot, boolean isAnalyze, boolean isSubquery ) {
        // Analyze logical query
        LogicalRelAnalyzeShuttle analyzeRelShuttle = new LogicalRelAnalyzeShuttle( statement );
        logicalRoot.rel.accept( analyzeRelShuttle );

        // Get partitions of logical information
        Map<Integer, Set<String>> partitionValueFilterPerScan = analyzeRelShuttle.getPartitionValueFilterPerScan();
        Map<Integer, List<Long>> accessedPartitionMap = this.getAccessedPartitionsPerTableScan( logicalRoot.rel, partitionValueFilterPerScan );

        // Build queryClass from query-name and partitions.
        String queryClass = analyzeRelShuttle.getQueryName();// + accessedPartitionMap;

        // Build LogicalQueryInformation instance and prepare monitoring
        LogicalQueryInformation queryInformation = new LogicalQueryInformationImpl(
                queryClass,
                accessedPartitionMap,
                analyzeRelShuttle.availableColumns,
                analyzeRelShuttle.availableColumnsWithTable,
                analyzeRelShuttle.getUsedColumns(),
                analyzeRelShuttle.getTables() );
        this.prepareMonitoring( statement, logicalRoot, isAnalyze, isSubquery, queryInformation );

        return queryInformation;
    }


    /**
     * Traverses all TablesScans used during execution and identifies for the corresponding table all
     * associated partitions that needs to be accessed, on the basis of the provided partitionValues identified in a LogicalFilter
     *
     * It is necessary to associate the partitionIds again with the TableScanId and not with the table itself. Because a table could be present
     * multiple times within one query. The aggregation per table would lead to data loss
     *
     * @param rel RelNode to be processed
     * @param aggregatedPartitionValues Mapping of TableScan Ids to identified partition Values
     * @return Mapping of TableScan Ids to identified partition Ids
     */
    private Map<Integer, List<Long>> getAccessedPartitionsPerTableScan( RelNode rel, Map<Integer, Set<String>> aggregatedPartitionValues ) {
        Map<Integer, List<Long>> accessedPartitionList = new HashMap<>(); // tableId  -> partitionIds
        if ( !(rel instanceof LogicalTableScan) ) {
            for ( int i = 0; i < rel.getInputs().size(); i++ ) {
                Map<Integer, List<Long>> result = getAccessedPartitionsPerTableScan( rel.getInput( i ), aggregatedPartitionValues );
                if ( !result.isEmpty() ) {
                    for ( Map.Entry<Integer, List<Long>> elem : result.entrySet() ) {
                        accessedPartitionList.merge( elem.getKey(), elem.getValue(), ( l1, l2 ) -> Stream.concat( l1.stream(), l2.stream() ).collect( Collectors.toList() ) );
                    }
                }
            }
        } else {
            boolean fallback = false;
            if ( rel.getTable() != null ) {
                RelOptTableImpl table = (RelOptTableImpl) rel.getTable();
                if ( table.getTable() instanceof LogicalTable ) {
                    LogicalTable logicalTable = ((LogicalTable) table.getTable());
                    int scanId = rel.getId();

                    // Get placements of this table
                    CatalogTable catalogTable = Catalog.getInstance().getTable( logicalTable.getTableId() );

                    if ( aggregatedPartitionValues.containsKey( scanId ) ) {
                        if ( aggregatedPartitionValues.get( scanId ) != null ) {
                            if ( !aggregatedPartitionValues.get( scanId ).isEmpty() ) {
                                List<String> partitionValues = new ArrayList<>( aggregatedPartitionValues.get( scanId ) );

                                if ( log.isDebugEnabled() ) {
                                    log.debug( "TableID: {} is partitioned on column: {} - {}",
                                            logicalTable.getTableId(),
                                            catalogTable.partitionColumnId,
                                            Catalog.getInstance().getColumn( catalogTable.partitionColumnId ).name );
                                }
                                List<Long> identifiedPartitions = new ArrayList<>();
                                for ( String partitionValue : partitionValues ) {
                                    if ( log.isDebugEnabled() ) {
                                        log.debug( "Extracted PartitionValue: {}", partitionValue );
                                    }
                                    long identifiedPartition = PartitionManagerFactory.getInstance()
                                            .getPartitionManager( catalogTable.partitionType )
                                            .getTargetPartitionId( catalogTable, partitionValue );

                                    identifiedPartitions.add( identifiedPartition );
                                    if ( log.isDebugEnabled() ) {
                                        log.debug( "Identified PartitionId: {} for value: {}", identifiedPartition, partitionValue );
                                    }
                                }

                                accessedPartitionList.merge( scanId, identifiedPartitions, ( l1, l2 ) -> Stream.concat( l1.stream(), l2.stream() ).collect( Collectors.toList() ) );
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


    private void prepareMonitoring( Statement statement, RelRoot logicalRoot, boolean isAnalyze, boolean isSubquery, LogicalQueryInformation queryInformation ) {
        // Initialize Monitoring
        if ( statement.getTransaction().getMonitoringEvent() == null ) {
            StatementEvent event;
            if ( logicalRoot.kind.belongsTo( SqlKind.DML ) ) {
                event = new DmlEvent();
            } else if ( logicalRoot.kind.belongsTo( SqlKind.QUERY ) ) {
                event = new QueryEvent();
            } else {
                log.error( "No corresponding monitoring event class found" );
                event = new QueryEvent();
            }

            event.setAnalyze( isAnalyze );
            event.setSubQuery( isSubquery );
            event.setLogicalQueryInformation( queryInformation );
            statement.getTransaction().setMonitoringEvent( event );
        }
    }


    private void monitorResult( ProposedRoutingPlan selectedPlan ) {
        if ( statement.getTransaction().getMonitoringEvent() != null ) {
            StatementEvent eventData = statement.getTransaction().getMonitoringEvent();
            eventData.setRelCompareString( selectedPlan.getRoutedRoot().rel.relCompareString() );
            if ( selectedPlan.getOptionalPhysicalQueryClass().isPresent() ) {
                eventData.setPhysicalQueryId( selectedPlan.getOptionalPhysicalQueryClass().get() );
                eventData.setRowCount( (int) selectedPlan.getRoutedRoot().rel.estimateRowCount( selectedPlan.getRoutedRoot().rel.getCluster().getMetadataQuery() ) );
            }

            if ( RoutingManager.POST_COST_AGGREGATION_ACTIVE.getBoolean() ) {
                if ( eventData instanceof QueryEvent ) {
                    // aggregate post costs
                    ((QueryEvent) eventData).setUpdatePostCosts( true );
                }
            }
            finalizeAccessedPartitions( eventData );
            MonitoringServiceProvider.getInstance().monitorEvent( eventData );
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
    }


    private void cacheRouterPlans( List<ProposedRoutingPlan> proposedRoutingPlans, List<RelOptCost> approximatedCosts, String queryId, Set<Long> partitionIds ) {
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


    private Pair<PolyphenyDbSignature<?>, ProposedRoutingPlan> selectPlan( ProposedImplementations proposedImplementations ) {
        // Lists should all be same size
        List<ProposedRoutingPlan> proposedRoutingPlans = proposedImplementations.getProposedRoutingPlans();
        List<RelNode> optimalRels = proposedImplementations.getOptimizedPlans();
        List<PolyphenyDbSignature<?>> signatures = proposedImplementations.getSignatures();
        LogicalQueryInformation queryInformation = proposedImplementations.getLogicalQueryInformation();

        List<RelOptCost> approximatedCosts;
        if ( RuntimeConfig.ROUTING_PLAN_CACHING.getBoolean() ) {
            // Get approximated costs and cache routing plans
            approximatedCosts = optimalRels.stream()
                    .map( rel -> rel.computeSelfCost( getPlanner(), rel.getCluster().getMetadataQuery() ) )
                    .collect( Collectors.toList() );
            this.cacheRouterPlans(
                    proposedRoutingPlans,
                    approximatedCosts,
                    queryInformation.getQueryClass(),
                    queryInformation.getAccessedPartitions().values().stream().flatMap( List::stream ).collect( Collectors.toSet() ) );
        }

        if ( signatures.size() == 1 ) {
            // If only one plan proposed, return this without further selection
            if ( statement.getTransaction().isAnalyze() ) {
                debugUiPrinter.printDebugOutputSingleResult( proposedRoutingPlans.get( 0 ), optimalRels.get( 0 ), statement );
            }

            return new Pair<>( signatures.get( 0 ), proposedRoutingPlans.get( 0 ) );
        }

        // Calculate costs and get selected plan from plan selector
        approximatedCosts = optimalRels.stream().map( rel -> rel.computeSelfCost( getPlanner(), rel.getCluster().getMetadataQuery() ) ).collect( Collectors.toList() );
        Pair<Optional<PolyphenyDbSignature<?>>, RoutingPlan> planPair = routingPlanSelector.selectPlanBasedOnCosts(
                proposedRoutingPlans,
                approximatedCosts,
                queryInformation.getQueryClass(),
                signatures,
                statement );

        if ( statement.getTransaction().isAnalyze() ) {
            int id = proposedRoutingPlans.indexOf( planPair.right );
            RelNode optimalNode = optimalRels.get( id );
            debugUiPrinter.printExecutedPhysicalPlan( optimalNode, statement );
        }

        return new Pair<>( planPair.left.get(), (ProposedRoutingPlan) planPair.right );
    }


    private CachedProposedRoutingPlan selectCachedPlan( List<CachedProposedRoutingPlan> routingPlansCached, String queryId ) {
        if ( routingPlansCached.size() == 1 ) {
            return routingPlansCached.get( 0 );
        }

        List<RelOptCost> approximatedCosts = routingPlansCached.stream().map( CachedProposedRoutingPlan::getPreCosts ).collect( Collectors.toList() );
        Pair<Optional<PolyphenyDbSignature<?>>, RoutingPlan> planPair = routingPlanSelector.selectPlanBasedOnCosts(
                routingPlansCached,
                approximatedCosts,
                queryId,
                statement );

        return (CachedProposedRoutingPlan) planPair.right;
    }

}
