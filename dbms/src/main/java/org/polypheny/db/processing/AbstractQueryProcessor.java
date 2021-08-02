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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
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
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.interpreter.Interpreters;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.monitoring.events.DMLEvent;
import org.polypheny.db.monitoring.events.QueryDataPoint;
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
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.routing.CachedProposedRoutingPlan;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.routing.ExecutionTimeMonitor.ExecutionTimeObserver;
import org.polypheny.db.routing.ProposedRoutingPlan;
import org.polypheny.db.routing.ProposedRoutingPlanImpl;
import org.polypheny.db.routing.QueryProcessorHelpers;
import org.polypheny.db.routing.RelDeepCopyShuttle;
import org.polypheny.db.routing.Router;
import org.polypheny.db.routing.RouterManager;
import org.polypheny.db.routing.RoutingPlan;
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
import oshi.util.tuples.Quartet;


@Slf4j
public abstract class AbstractQueryProcessor implements QueryProcessor, ExecutionTimeObserver {

    // region final fields

    protected static final boolean ENABLE_BINDABLE = false;
    protected static final boolean ENABLE_COLLATION_TRAIT = true;
    protected static final boolean ENABLE_ENUMERABLE = true;
    protected static final boolean CONSTANT_REDUCTION = false;
    protected static final boolean ENABLE_STREAM = true;
    private final Statement statement;

    // endregion

    // region ctor


    protected AbstractQueryProcessor( Statement statement ) {
        this.statement = statement;
    }

    // endregion


    // region public overrides
    @Override
    public void executionTime( String reference, long nanoTime ) {
        val event = (StatementEvent) statement.getTransaction().getMonitoringEvent();
        if ( reference.equals( event.getQueryId() ) ) {
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
        statement.getShortRunningRouters().forEach( Router::resetCaches );
        statement.getLongRunningRouters().forEach( Router::resetCaches );
    }


    @Override
    public PolyphenyDbSignature prepareQuery( RelRoot logicalRoot, boolean withMonitoring ) {
        return prepareQuery(
                logicalRoot,
                logicalRoot.rel.getCluster().getTypeFactory().builder().build(),
                false, withMonitoring );
    }


    @Override
    public PolyphenyDbSignature prepareQuery( RelRoot logicalRoot, RelDataType parameterRowType, boolean isRouted, boolean withMonitoring ) {
        return prepareQuery( logicalRoot, parameterRowType, isRouted, false, withMonitoring );

    }

    // endregion


    private void monitorResult( ProposedRoutingPlan selectedPlan ) {
        if ( statement.getTransaction().getMonitoringEvent() != null ) {
            StatementEvent eventData = (StatementEvent) statement.getTransaction().getMonitoringEvent();
            eventData.setDurations( statement.getDuration().asJson() );
            eventData.setRelCompareString( selectedPlan.getRoutedRoot().rel.relCompareString() );
            if ( selectedPlan.getOptionalPhysicalQueryId().isPresent() ) {
                eventData.setPhysicalQueryId( selectedPlan.getOptionalPhysicalQueryId().get() );
            }

            MonitoringServiceProvider.getInstance().monitorEvent( eventData );
        }
    }

    // region processing


    private PolyphenyDbSignature prepareQuery( RelRoot logicalRoot, RelDataType parameterRowType, boolean isRouted, boolean isSubquery, boolean withMonitoring ) {
        val proposedResults = prepareQueryList( logicalRoot, parameterRowType, isRouted, isSubquery );

        if ( statement.getTransaction().isAnalyze() ) {
            statement.getDuration().start( "Plan Selection" );
        }

        val executionResult = selectPlan( proposedResults );

        if ( statement.getTransaction().isAnalyze() ) {
            statement.getDuration().stop( "Plan Selection" );
        }

        // TODO: Calc costs depending on parameters
        // TODO: get real rowNumbers in selfCost
        // TODO: find index on store for costs

        if ( withMonitoring ) {
            this.monitorResult( executionResult.right );
        }

        return executionResult.left;
    }


    private Quartet<List<ProposedRoutingPlan>, List<RelNode>, List<PolyphenyDbSignature>, String>

    prepareQueryList( RelRoot logicalRoot, RelDataType parameterRowType, boolean isRouted, boolean isSubquery ) {
        boolean isAnalyze = statement.getTransaction().isAnalyze() && !isSubquery;
        boolean lock = !isSubquery;
        final Convention resultConvention = ENABLE_BINDABLE ? BindableConvention.INSTANCE : EnumerableConvention.INSTANCE;
        final StopWatch stopWatch = new StopWatch();
        List<ProposedRoutingPlan> proposedRoutingPlans = null;
        List<Optional<RelNode>> optimalNodeList = new ArrayList<>();
        List<RelRoot> parameterizedRootList = new ArrayList<>();
        List<Optional<PolyphenyDbSignature>> signatures = new ArrayList<>();

        stopWatch.start();

        // check for view
        if ( logicalRoot.rel.hasView() ) {
            logicalRoot = logicalRoot.tryExpandView();
        }

        // Index Update
        if ( isAnalyze ) {
            statement.getDuration().start( "Analyze" );
        }

        // analyze query, get logical partitions, queryId and initialize monitoring
        val queryId = this.analyzeQueryAndPrepareMonitoring( statement, logicalRoot, isAnalyze, isSubquery );

        if ( isAnalyze ) {
            statement.getDuration().stop( "Analyze" );
        }

        ExecutionTimeMonitor executionTimeMonitor = new ExecutionTimeMonitor();
        executionTimeMonitor.subscribe( this, queryId );

        if ( isRouted ) {
            proposedRoutingPlans = Lists.newArrayList( new ProposedRoutingPlanImpl( logicalRoot, queryId ) );
        } else {
            if ( lock ) {
                this.acquireLock( isAnalyze, logicalRoot );
            }

            // Index Update
            if ( isAnalyze ) {
                statement.getDuration().stop( "Locking" );
                statement.getDuration().start( "Index Update" );
            }
            RelRoot indexUpdateRoot = logicalRoot;
            if ( RuntimeConfig.POLYSTORE_INDEXES_ENABLED.getBoolean() ) {
                IndexManager.getInstance().barrier( statement.getTransaction().getXid() );
                indexUpdateRoot = indexUpdate( indexUpdateRoot, statement, parameterRowType );
            }

            // Constraint Enforcement Rewrite
            if ( isAnalyze ) {
                statement.getDuration().stop( "Index Update" );
                statement.getDuration().start( "Constraint Enforcement" );
            }
            RelRoot constraintsRoot = indexUpdateRoot;
            if ( RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean() || RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() ) {
                ConstraintEnforcer constraintEnforcer = new EnumerableConstraintEnforcer();
                constraintsRoot = constraintEnforcer.enforce( constraintsRoot, statement );
            }

            // Index Lookup Rewrite
            if ( isAnalyze ) {
                statement.getDuration().stop( "Constraint Enforcement" );
                statement.getDuration().start( "Index Lookup Rewrite" );
            }
            RelRoot indexLookupRoot = constraintsRoot;
            if ( RuntimeConfig.POLYSTORE_INDEXES_ENABLED.getBoolean() && RuntimeConfig.POLYSTORE_INDEXES_SIMPLIFY.getBoolean() ) {
                indexLookupRoot = indexLookup( indexLookupRoot, statement );
            }

            if ( isAnalyze ) {
                statement.getDuration().stop( "Index Lookup Rewrite" );
            }

            if ( RuntimeConfig.ROUTING_PLAN_CACHING.getBoolean() &&
                    !indexLookupRoot.kind.belongsTo( SqlKind.DML )
            ) {
                val routingPlansCached = RoutingPlanCache.INSTANCE.getIfPresent( queryId );
                if ( !routingPlansCached.isEmpty() ) {
                    proposedRoutingPlans = routeCached( indexLookupRoot, routingPlansCached, statement, queryId, isAnalyze );
                }
            }

            if(proposedRoutingPlans == null) {
                if ( isAnalyze ) {
                    statement.getDuration().start( "Routing" );
                }
                proposedRoutingPlans = route( indexLookupRoot, statement, queryId );
            }

            proposedRoutingPlans.forEach( proposedRoutingPlan -> {
                val routedRoot = proposedRoutingPlan.getRoutedRoot();
                RelStructuredTypeFlattener typeFlattener = new RelStructuredTypeFlattener(
                        RelBuilder.create( statement, routedRoot.rel.getCluster() ),
                        routedRoot.rel.getCluster().getRexBuilder(),
                        ViewExpanders.toRelContext( this, routedRoot.rel.getCluster() ),
                        true );
                proposedRoutingPlan.setRoutedRoot( routedRoot.withRel( typeFlattener.rewrite( routedRoot.rel ) ) );
            } );

            if ( isAnalyze ) {
                statement.getDuration().stop( "Routing" );
            }
        }

        // Validate parameterValues
        proposedRoutingPlans.forEach( proposedRoutingPlan ->
                new ParameterValueValidator( proposedRoutingPlan.getRoutedRoot().validatedRowType, statement.getDataContext() )
                        .visit( proposedRoutingPlan.getRoutedRoot().rel ) );

        //
        // Parameterize

        // Add optional parameterizedRoots and signatures for all routed RelRoots.
        // Index of routedRoot, parameterizedRootList and signatures correspond!

        for ( ProposedRoutingPlan routingPlan : proposedRoutingPlans ) {
            val routedRoot = routingPlan.getRoutedRoot();
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

        //
        // Implementation Caching
        if ( isAnalyze ) {
            statement.getDuration().start( "Implementation Caching" );
        }

        for ( int i = 0; i < proposedRoutingPlans.size(); i++ ) {
            val routedRoot = proposedRoutingPlans.get( i ).getRoutedRoot();
            if ( this.isImplementationCachingActive( statement, routedRoot ) ) {

                val parameterizedRoot = parameterizedRootList.get( i );
                PreparedResult preparedResult = ImplementationCache.INSTANCE.getIfPresent( parameterizedRoot.rel );
                if ( preparedResult != null ) {
                    PolyphenyDbSignature signature = createSignature( preparedResult, routedRoot, resultConvention, executionTimeMonitor );
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
            statement.getDuration().stop( "Implementation Caching" );
        }

        // can we return earlier?
        if ( signatures.stream().allMatch( Optional::isPresent ) &&
                optimalNodeList.stream().allMatch( Optional::isPresent )
        ) {
            return new Quartet(
                    proposedRoutingPlans,
                    optimalNodeList.stream().filter( Optional::isPresent ).map( Optional::get ).collect( Collectors.toList() ),
                    signatures.stream().filter( Optional::isPresent ).map( Optional::get ).collect( Collectors.toList() ),
                    queryId );
        }

        assert (signatures.size() == proposedRoutingPlans.size());
        assert (optimalNodeList.size() == proposedRoutingPlans.size());
        assert (parameterizedRootList.size() == proposedRoutingPlans.size());

        // Plan Caching
        if ( isAnalyze ) {
            statement.getDuration().start( "Plan Caching" );
        }
        for ( int i = 0; i < proposedRoutingPlans.size(); i++ ) {
            if ( optimalNodeList.get( i ).isPresent() ) {
                continue;
            }
            val routedRoot = proposedRoutingPlans.get( i ).getRoutedRoot();

            if ( this.isQueryPlanCachingActive( statement, routedRoot ) ) {
                // should always be the case
                val cachedElem = QueryPlanCache.INSTANCE.getIfPresent( parameterizedRootList.get( i ).rel );
                if ( cachedElem != null ) {
                    optimalNodeList.set( i, Optional.of( cachedElem ) );
                }
            }
        }
        //
        // Planning & Optimization
        if ( isAnalyze ) {
            statement.getDuration().stop( "Plan Caching" );
            statement.getDuration().start( "Planning & Optimization" );
        }

        // optimalNode same size as routed, parametrized and signature
        for ( int i = 0; i < optimalNodeList.size(); i++ ) {
            val parameterizedRoot = parameterizedRootList.get( i );
            val routedRoot = proposedRoutingPlans.get( i ).getRoutedRoot();
            if ( !optimalNodeList.get( i ).isPresent() ) {
                optimalNodeList.set( i, Optional.of( optimize( parameterizedRoot, resultConvention ) ) );
                if ( this.isQueryPlanCachingActive( statement, routedRoot ) ) {
                    QueryPlanCache.INSTANCE.put( parameterizedRoot.rel, optimalNodeList.get( i ).get() );
                }
            }
        }

        //
        // Implementation
        if ( isAnalyze ) {
            statement.getDuration().stop( "Planning & Optimization" );
            statement.getDuration().start( "Implementation" );
        }

        for ( int i = 0; i < optimalNodeList.size(); i++ ) {
            if ( signatures.get( i ).isPresent() ) {
                continue;
            }

            val optimalNode = optimalNodeList.get( i ).get();
            val parameterizedRoot = parameterizedRootList.get( i );
            val routedRoot = proposedRoutingPlans.get( i ).getRoutedRoot();

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

            signatures.set( i, Optional.of( createSignature( preparedResult, optimalRoot, resultConvention, executionTimeMonitor ) ) );
            optimalNodeList.set( i, Optional.of( optimalRoot.rel ) );
        }
        if ( isAnalyze ) {
            statement.getDuration().stop( "Implementation" );
        }

        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Preparing statement ... done. [{}]", stopWatch );
        }

        return new Quartet(
                proposedRoutingPlans,
                optimalNodeList.stream().filter( Optional::isPresent ).map( Optional::get ).collect( Collectors.toList() ),
                signatures.stream().filter( Optional::isPresent ).map( Optional::get ).collect( Collectors.toList() ),
                queryId );
    }

    // endregion

    // region processing single steps


    private void acquireLock( boolean isAnalyze, RelRoot logicalRoot ) {
        // Locking
        if ( isAnalyze ) {
            statement.getDuration().start( "Locking" );
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
                            final PolyphenyDbSignature scanSig = prepareQuery( scanRoot, parameterRowType, false, true );
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


    private List<ProposedRoutingPlan> route( RelRoot logicalRoot, Statement statement, String queryId ) {
        InformationPage page = null;
        log.info( "Start Routing" );

        if ( logicalRoot.rel instanceof LogicalTableModify ) {
            val routedDml = RouterManager.getInstance().getDmlRouter().routeDml( logicalRoot.rel, statement );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedDml, logicalRoot, queryId ) );
        } else if ( logicalRoot.rel instanceof ConditionalExecute ) {
            val routedConditionalExecute = RouterManager.getInstance().getDmlRouter().handleConditionalExecute( logicalRoot.rel, statement, RouterManager.getInstance().getFallbackRouter() );
            return Lists.newArrayList( new ProposedRoutingPlanImpl( routedConditionalExecute, logicalRoot, queryId ) );
        } else {
            log.info( "Start build DQL" );

            // TODO
            if ( !RouterManager.IS_LONG_ACTIVE.getBoolean() ) {
                val proposedPlans = new ArrayList<ProposedRoutingPlan>();
                for ( val router : RouterManager.getInstance().getShortRunningRouters() ) {
                    val builders = router.route( logicalRoot, statement );
                    proposedPlans.addAll(
                            builders.stream().map( builder ->
                                    new ProposedRoutingPlanImpl( builder, logicalRoot, queryId, router.getClass() )
                            ).collect( Collectors.toList() )
                    );

                }

                return proposedPlans;
            }
            // TODO: long running queries

            //routed = builders.stream().map( RelBuilder::build ).collect( Collectors.toList() );
            log.info( "End DQL" );
        }

        throw new IllegalStateException( "This point should never be reached!" );
    }


    private List<ProposedRoutingPlan> routeCached( RelRoot logicalRoot, List<CachedProposedRoutingPlan> routingPlansCached, Statement statement, String queryId, boolean isAnalyze ) {
        // todo: get only best plan.

        if ( isAnalyze ) {
            statement.getDuration().start( "Plan Selection" );
        }

        val selectedCachedPlan = selectCachedPlan( routingPlansCached, queryId );

        if ( isAnalyze ) {
            statement.getDuration().stop( "Plan Selection" );
        }

        if ( isAnalyze ) {
            statement.getDuration().start( "Routing" );
        }

        val builder = RouterManager.getInstance().getCachedPlanRouter().routeCached( logicalRoot, selectedCachedPlan, statement );

        val proposed = new ProposedRoutingPlanImpl( builder, logicalRoot, queryId, selectedCachedPlan );

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
        if ( statement.getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "Physical Query Plan" ).setLabel( "plans" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Physical Query Plan" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    group,
                    RelOptUtil.dumpPlan( "Physical Query Plan", root.rel, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
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

    // endregion

    // region private helpers


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
        return RuntimeConfig.QUERY_PLAN_CACHING.getBoolean() &&
                (
                        !relRoot.kind.belongsTo( SqlKind.DML ) ||
                                RuntimeConfig.QUERY_PLAN_CACHING_DML.getBoolean() ||
                                statement.getDataContext().getParameterValues().size() > 0
                );
    }


    private boolean isImplementationCachingActive( Statement statement, RelRoot relRoot ) {
        return RuntimeConfig.IMPLEMENTATION_CACHING.getBoolean() &&
                (
                        !relRoot.kind.belongsTo( SqlKind.DML ) ||
                                RuntimeConfig.IMPLEMENTATION_CACHING_DML.getBoolean() ||
                                statement.getDataContext().getParameterValues().size() > 0
                );
    }


    private String analyzeQueryAndPrepareMonitoring( Statement statement, RelRoot logicalRoot, boolean isAnalyze, boolean isSubquery ) {
        val analyzeRelShuttle = new LogicalRelQueryAnalyzeShuttle( statement );
        logicalRoot.rel.accept( analyzeRelShuttle );

        List<String> partitionValues = analyzeRelShuttle.filterMap.values().stream().flatMap( Collection::stream ).collect( Collectors.toList() );

        var accessedPartitionMap = this.getAccessedPartitionsPerTable( logicalRoot.rel, partitionValues );

        String queryId = analyzeRelShuttle.getQueryName() + accessedPartitionMap;

        this.prepareMonitoring( analyzeRelShuttle, statement, logicalRoot, accessedPartitionMap, queryId, isAnalyze, isSubquery );

        return queryId;
    }


    private Map<Long, List<Long>> getAccessedPartitionsPerTable( RelNode rel, List<String> partitionValues ) {
        Map<Long, List<Long>> accessedPartitionList = new HashMap<>();
        if ( !(rel instanceof LogicalTableScan) ) {
            for ( int i = 0; i < rel.getInputs().size(); i++ ) {
                val result = getAccessedPartitionsPerTable( rel.getInput( i ), partitionValues );
                if ( !result.isEmpty() ) {
                    for ( val elem : result.entrySet() ) {
                        accessedPartitionList.merge( elem.getKey(), elem.getValue(), ( l1, l2 ) -> Stream.concat( l1.stream(), l2.stream() ).collect( Collectors.toList() ) );
                    }
                }

            }
        } else {
            if ( rel.getTable() != null ) {
                RelOptTableImpl table = (RelOptTableImpl) rel.getTable();
                if ( table.getTable() instanceof LogicalTable ) {
                    LogicalTable logicalTable = ((LogicalTable) table.getTable());
                    // Get placements of this table
                    CatalogTable catalogTable = Catalog.getInstance().getTable( logicalTable.getTableId() );

                    if ( !partitionValues.isEmpty() ) {
                        if ( log.isDebugEnabled() ) {
                            log.debug( "TableID: {} is partitioned on column: {} - {}",
                                    logicalTable.getTableId(),
                                    catalogTable.partitionColumnId,
                                    Catalog.getInstance().getColumn( catalogTable.partitionColumnId ).name );
                        }
                        List<Long> identifiedPartitions = new ArrayList<>();
                        for ( String partitionValue : partitionValues ) {
                            log.debug( "Extracted PartitionValue: {}", partitionValue );
                            long identifiedPartition = PartitionManagerFactory.getInstance()
                                    .getPartitionManager( catalogTable.partitionType )
                                    .getTargetPartitionId( catalogTable, partitionValue );

                            identifiedPartitions.add( identifiedPartition );
                            log.debug( "Identified PartitionId: {} for value: {}", identifiedPartition, partitionValue );
                        }

                        // Currently only one partition is identified, therefore LIST is not needed YET.
                        accessedPartitionList.merge( catalogTable.id, identifiedPartitions, ( l1, l2 ) -> Stream.concat( l1.stream(), l2.stream() ).collect( Collectors.toList() ) );
                        // fallback
                    } else {
                        accessedPartitionList.merge( catalogTable.id, catalogTable.partitionProperty.partitionIds, ( l1, l2 ) -> Stream.concat( l1.stream(), l2.stream() ).collect( Collectors.toList() ) );
                    }
                }
            }
        }
        return accessedPartitionList;
    }


    private void prepareMonitoring( LogicalRelQueryAnalyzeShuttle shuttle, Statement statement, RelRoot logicalRoot, Map<Long, List<Long>> accessedPartitionList, String queryId, boolean isAnalyze, boolean isSubquery ) {

        // initialize Monitoring
        if ( statement.getTransaction().getMonitoringEvent() == null ) {
            StatementEvent event;
            if ( logicalRoot.kind.belongsTo( SqlKind.DML ) ) {
                event = new DMLEvent();

            } else if ( logicalRoot.kind.belongsTo( SqlKind.QUERY ) ) {
                event = new QueryEvent();
            } else {
                log.error( "No corresponding monitoring event class found" );
                event = new QueryEvent();
                return;
            }

            event.setFieldNames( ImmutableList.copyOf( shuttle.availableColumns.values() ) );
            event.setAnalyze( isAnalyze );
            event.setSubQuery( isSubquery );

            event.setAccessedPartitions( accessedPartitionList );
            event.setAnalyzeRelShuttle( shuttle );
            event.setQueryId( queryId );
            statement.getTransaction().setMonitoringEvent( event );
        }
    }


    private void cacheRouterPlans( List<ProposedRoutingPlan> proposedRoutingPlans, List<RelOptCost> approximatedCosts, String queryId ) {
        val cachedPlans = new ArrayList<CachedProposedRoutingPlan>();
        for ( int i = 0; i < proposedRoutingPlans.size(); i++ ) {
            if ( proposedRoutingPlans.get( i ).isCachable() && !RoutingPlanCache.INSTANCE.isKeyPresent( queryId )) {
                cachedPlans.add(
                        new CachedProposedRoutingPlan( proposedRoutingPlans.get( i ), approximatedCosts.get( i ) )
                );
            }
        }

        if ( !cachedPlans.isEmpty() ) {
            RoutingPlanCache.INSTANCE.put( queryId, cachedPlans );
        }
    }

    // endregion

    // region plan selection with cost calculation


    private CachedProposedRoutingPlan selectCachedPlan( List<CachedProposedRoutingPlan> routingPlansCached, String queryId ) {
        if ( routingPlansCached.size() == 1 ) {
            return routingPlansCached.get( 0 );
        }

        val approximatedCosts = routingPlansCached.stream().map( CachedProposedRoutingPlan::getPreCosts ).collect( Collectors.toList() );
        var planPair = selectPlanBasedOnCosts( routingPlansCached, approximatedCosts, queryId, Optional.empty() );

        return (CachedProposedRoutingPlan) planPair.right;

    }


    private Pair<PolyphenyDbSignature, ProposedRoutingPlan> selectPlan( Quartet<List<ProposedRoutingPlan>, List<RelNode>, List<PolyphenyDbSignature>, String> proposedResults ) {
        // lists should all be same size
        val proposedRoutingPlans = proposedResults.getA();
        val optimalRels = proposedResults.getB();
        val signatures = proposedResults.getC();
        val queryId = proposedResults.getD();

        val approximatedCosts = optimalRels.stream().map( rel -> rel.computeSelfCost( getPlanner(), RelMetadataQuery.instance() ) ).collect( Collectors.toList() );
        this.cacheRouterPlans( proposedRoutingPlans, approximatedCosts, queryId );

        if ( signatures.size() == 1 ) {
            return new Pair<>( signatures.get( 0 ), proposedRoutingPlans.get( 0 ) );
        }

        var planPair = selectPlanBasedOnCosts( proposedRoutingPlans, approximatedCosts, queryId, Optional.of( signatures ) );

        return new Pair<>( planPair.left.get(), (ProposedRoutingPlan) planPair.right );
    }


    private Pair<Optional<PolyphenyDbSignature>, RoutingPlan> selectPlanBasedOnCosts( List<? extends RoutingPlan> routingPlans, List<RelOptCost> approximatedCosts, String queryId, Optional<List<PolyphenyDbSignature>> signatures ) {
        // 0 = only consider pre costs
        // 1 = only consider post costs
        // [0,1] = get normalized pre and post costs and multiply by ratio
        val ratioPre = 1 - RouterManager.PRE_COST_POST_COST_RATIO.getDouble();
        val rationPost = RouterManager.PRE_COST_POST_COST_RATIO.getDouble();
        val n = routingPlans.size();

        val calcPreCosts = Math.abs( ratioPre ) >= RelOptUtil.EPSILON; // <=0
        List<Double> preCosts = calcPreCosts ? normalizeApproximatedCosts( approximatedCosts ) : Collections.nCopies( n, 0.0 );

        val calcPostCosts = Math.abs( ratioPre ) >= RelOptUtil.EPSILON; // > 0
        Optional<List<Double>> icarusCosts;
        List<Double> postCosts;
        if ( calcPostCosts ) {
            val icarusResult = calcIcarusPostCosts( routingPlans, queryId );
            icarusCosts = Optional.of( icarusResult.right );
            postCosts = icarusResult.left;
        } else {
            postCosts = Collections.nCopies( n, 0.0 );
            icarusCosts = Optional.empty();
        }

        RoutingPlan currentPlan = routingPlans.get( 0 );
        Optional<PolyphenyDbSignature> currentSignature = signatures.isPresent() ? Optional.of( signatures.get().get( 0 ) ) : Optional.empty();
        double currentCost = (preCosts.get( 0 ) * ratioPre) + (postCosts.get( 0 ) * rationPost);
        for ( int i = 1; i < routingPlans.size(); i++ ) {
            val cost = (preCosts.get( i ) * ratioPre) + (postCosts.get( i ) * rationPost);
            if ( cost < currentCost ) {
                currentPlan = routingPlans.get( i );
                currentSignature = signatures.isPresent() ? Optional.of( signatures.get().get( i ) ) : Optional.empty();
                currentCost = cost;
            }
        }

        if ( statement.getTransaction().isAnalyze() ) {
            this.printDebugOutput( approximatedCosts, preCosts, postCosts, icarusCosts, routingPlans, ratioPre, rationPost, currentPlan );
        }

        return new Pair<>( currentSignature, currentPlan );
    }


    private void printDebugOutput(
            List<RelOptCost> approximatedCosts,
            List<Double> preCosts,
            List<Double> postCosts,
            Optional<List<Double>> icarusCosts,
            List<? extends RoutingPlan> routingPlans,
            Double ratioPre,
            Double ratioPost,
            RoutingPlan selectedPlan ) {

        // initialize information for query analyzer
        InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
        var page = new InformationPage( "Routing" );
        page.fullWidth();
        queryAnalyzer.addPage( page );

        InformationGroup overview = new InformationGroup( page, "Stats overview" );
        statement.getTransaction().getQueryAnalyzer().addGroup( overview );
        InformationTable overviewTable = new InformationTable( overview, ImmutableList.of( "# Proposed plans", "Pre Cost factor", "Post cost factor" ) );
        overviewTable.addRow( routingPlans.size(), ratioPre, ratioPost );
        statement.getTransaction().getQueryAnalyzer().registerInformation( overviewTable );

        val isIcarus = icarusCosts.isPresent();

        InformationGroup group = new InformationGroup( page, "Proposed Plans" );
        statement.getTransaction().getQueryAnalyzer().addGroup( group );
        InformationTable table = new InformationTable(
                group,
                ImmutableList.of( "Physical Query Id", "router", "Approx. Costs", "Norm. approx Costs", "Icarus Costs", "Norm. Icarus Costs", "Partitioning - Placements" ) );

        for ( int i = 0; i < routingPlans.size(); i++ ) {
            val routingPlan = routingPlans.get( i );
            table.addRow(
                    routingPlan.getPhysicalQueryId(),
                    routingPlan.getRouter(),
                    approximatedCosts.get( i ),
                    preCosts.get( i ),
                    isIcarus ? icarusCosts.get().get( i ) : "-",
                    isIcarus ? postCosts.get( i ) : "-",
                    routingPlan.getOptionalPhysicalPlacementsOfPartitions() ); //.isPresent() ? routingPlan.getOptionalPhysicalPlacementsOfPartitions().get(): "-"
        }

        statement.getTransaction().getQueryAnalyzer().registerInformation( table );

        InformationGroup selected = new InformationGroup( page, "Selected Plan" );
        statement.getTransaction().getQueryAnalyzer().addGroup( selected );
        InformationTable selectedTable = new InformationTable( selected, ImmutableList.of( "QueryId", "Physical QueryId", "Router", "Partitioning - Placements" ) );
        selectedTable.addRow(
                selectedPlan.getQueryId(),
                selectedPlan.getPhysicalQueryId(),
                selectedPlan.getRouter(),
                selectedPlan.getOptionalPhysicalPlacementsOfPartitions() );

        statement.getTransaction().getQueryAnalyzer().registerInformation( selectedTable );
    }


    private List<Double> normalizeCots( List<Double> costs ) {
        val min = costs.stream().min( Double::compareTo );
        val max = costs.stream().max( Double::compareTo );
        if ( !min.isPresent() || !max.isPresent() ) {
            log.error( "should never happen" );
        }

        // when min == mix, set min = 0
        val usedMin = Math.abs( min.get() - max.get() ) <= RelOptUtil.EPSILON ? Optional.of( 0.0 ) : min;
        return costs.stream().map( c -> (c - usedMin.get()) / (max.get() - usedMin.get()) ).collect( Collectors.toList() );
    }


    private List<Double> normalizeApproximatedCosts( List<RelOptCost> approximatedCosts ) {
        val costs = approximatedCosts.stream().map( RelOptCost::getCosts ).collect( Collectors.toList() );
        return normalizeCots( costs );
    }


    private Pair<List<Double>, List<Double>> calcIcarusPostCosts( List<? extends RoutingPlan> proposedRoutingPlans, String queryId ) {
        val measuredData = MonitoringServiceProvider.getInstance().getQueryDataPoints( queryId );
        val icarusCosts = new ArrayList<Double>();

        for ( int i = 0; i < proposedRoutingPlans.size(); i++ ) {
            val plan = proposedRoutingPlans.get( i );
            val dataPoints = measuredData.stream()
                    .filter( elem -> elem.getPhysicalQueryId().equals( plan.getPhysicalQueryId() ) )
                    .collect( Collectors.toList() );
            if ( dataPoints.isEmpty() ) {
                icarusCosts.add( 0.0 );
            } else {
                val value = dataPoints.stream()
                        .map( QueryDataPoint::getExecutionTime )
                        .mapToDouble( d -> d )
                        .average();

                if ( value.isPresent() ) {
                    icarusCosts.add( value.getAsDouble() );
                } else {
                    icarusCosts.add( 0.0 );
                }
            }
        }

        // normalize values
        val normalized = normalizeCots( icarusCosts );
        return new Pair<>( normalized, icarusCosts );
    }

    // endregion
}
