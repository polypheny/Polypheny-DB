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
import java.lang.reflect.Type;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Ord;
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
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.SchemaTypeVisitor;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.core.DeadlockException;
import org.polypheny.db.core.enums.ExplainFormat;
import org.polypheny.db.core.enums.ExplainLevel;
import org.polypheny.db.core.enums.Kind;
import org.polypheny.db.core.rel.RelStructuredTypeFlattener;
import org.polypheny.db.core.util.Conformance;
import org.polypheny.db.document.util.DataModelShuttle;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.interpreter.Interpreters;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.monitoring.events.DmlEvent;
import org.polypheny.db.monitoring.events.QueryEvent;
import org.polypheny.db.monitoring.events.StatementEvent;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.plan.ViewExpanders;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.prepare.Prepare.PreparedResult;
import org.polypheny.db.prepare.Prepare.PreparedResultImpl;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.RelShuttle;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.core.ConditionalExecute.Condition;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.core.TableFunctionScan;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.logical.LogicalAggregate;
import org.polypheny.db.rel.logical.LogicalConditionalExecute;
import org.polypheny.db.rel.logical.LogicalCorrelate;
import org.polypheny.db.rel.logical.LogicalExchange;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalIntersect;
import org.polypheny.db.rel.logical.LogicalJoin;
import org.polypheny.db.rel.logical.LogicalMatch;
import org.polypheny.db.rel.logical.LogicalMinus;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalSort;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalUnion;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.Typed;
import org.polypheny.db.tools.Program;
import org.polypheny.db.tools.Programs;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TableAccessMap;
import org.polypheny.db.transaction.TableAccessMap.Mode;
import org.polypheny.db.transaction.TableAccessMap.TableIdentifier;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.ExtraPolyTypes;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import org.polypheny.db.view.MaterializedViewManager;
import org.polypheny.db.view.MaterializedViewManager.TableUpdateVisitor;
import org.polypheny.db.view.ViewManager.ViewVisitor;


@Slf4j
public abstract class AbstractQueryProcessor implements QueryProcessor {

    protected static final boolean ENABLE_BINDABLE = false;
    protected static final boolean ENABLE_COLLATION_TRAIT = true;
    protected static final boolean ENABLE_ENUMERABLE = true;
    protected static final boolean CONSTANT_REDUCTION = false;
    protected static final boolean ENABLE_STREAM = true;

    private final Statement statement;


    protected AbstractQueryProcessor( Statement statement ) {
        this.statement = statement;
    }


    @Override
    public PolyphenyDbSignature prepareQuery( RelRoot logicalRoot ) {
        return prepareQuery( logicalRoot, logicalRoot.rel.getCluster().getTypeFactory().builder().build(), false );
    }


    @Override
    public PolyphenyDbSignature prepareQuery( RelRoot logicalRoot, RelDataType parameterRowType, boolean isRouted ) {
        return prepareQuery( logicalRoot, parameterRowType, isRouted, false );
    }


    @Override
    public PolyphenyDbSignature prepareQuery( RelRoot logicalRoot, RelDataType parameterRowType, boolean isRouted, boolean isSubquery ) {
        return prepareQuery( logicalRoot, parameterRowType, isRouted, isSubquery, false );
    }


    @Override
    public PolyphenyDbSignature prepareQuery( RelRoot logicalRoot, RelDataType parameterRowType, boolean isRouted, boolean isSubquery, boolean doesSubstituteOrderBy ) {
        boolean isAnalyze = statement.getTransaction().isAnalyze() && !isSubquery;
        boolean lock = !isSubquery;
        SchemaType schemaType = null;

        final StopWatch stopWatch = new StopWatch();

        if ( log.isDebugEnabled() ) {
            log.debug( "Preparing statement  ..." );
        }

        if ( statement.getTransaction().getMonitoringData() == null ) {
            if ( logicalRoot.kind.belongsTo( Kind.DML ) ) {
                statement.getTransaction().setMonitoringData( new DmlEvent() );
            } else if ( logicalRoot.kind.belongsTo( Kind.QUERY ) ) {
                statement.getTransaction().setMonitoringData( new QueryEvent() );
            }
        }

        stopWatch.start();

        logicalRoot.rel.accept( new DataModelShuttle() );

        ExecutionTimeMonitor executionTimeMonitor = new ExecutionTimeMonitor();

        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Prepare Views" );
        }

        /*
        check if the relRoot includes Views or Materialized Views and replaces what necessary
        View: replace LogicalViewTableScan with underlying information
        Materialized View: add order by if Materialized View includes Order by */
        ViewVisitor viewVisitor = new ViewVisitor( doesSubstituteOrderBy );
        logicalRoot = viewVisitor.startSubstitution( logicalRoot );

        //Update which tables where changed used for Materialized Views
        TableUpdateVisitor visitor = new TableUpdateVisitor();
        logicalRoot.rel.accept( visitor );
        MaterializedViewManager.getInstance().addTables( statement.getTransaction(), visitor.getNames() );

        SchemaTypeVisitor schemaTypeVisitor = new SchemaTypeVisitor();
        logicalRoot.rel.accept( schemaTypeVisitor );
        schemaType = schemaTypeVisitor.getSchemaTypes();

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Prepare Views" );
        }

        final Convention resultConvention =
                ENABLE_BINDABLE
                        ? BindableConvention.INSTANCE
                        : EnumerableConvention.INSTANCE;

        RelRoot routedRoot;
        if ( !isRouted ) {
            if ( lock ) {
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
                indexLookupRoot = indexLookup( indexLookupRoot, statement, executionTimeMonitor );
            }

            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Index Lookup Rewrite" );
                statement.getProcessingDuration().start( "Routing" );
            }
            routedRoot = route( indexLookupRoot, statement, executionTimeMonitor );

            RelStructuredTypeFlattener typeFlattener = new RelStructuredTypeFlattener(
                    RelBuilder.create( statement, routedRoot.rel.getCluster() ),
                    routedRoot.rel.getCluster().getRexBuilder(),
                    ViewExpanders.toRelContext( this, routedRoot.rel.getCluster() ),
                    true );
            routedRoot = routedRoot.withRel( typeFlattener.rewrite( routedRoot.rel ) );
            if ( isAnalyze ) {
                statement.getProcessingDuration().stop( "Routing" );
            }
        } else {
            routedRoot = logicalRoot;
        }

        // Validate parameterValues
        ParameterValueValidator pmValidator = new ParameterValueValidator( routedRoot.validatedRowType, statement.getDataContext() );
        pmValidator.visit( routedRoot.rel );

        //
        // Parameterize
        RelRoot parameterizedRoot = null;
        if ( statement.getDataContext().getParameterValues().size() == 0 && (RuntimeConfig.PARAMETERIZE_DML.getBoolean() || !routedRoot.kind.belongsTo( Kind.DML )) ) {
            Pair<RelRoot, RelDataType> parameterized = parameterize( routedRoot, parameterRowType );
            parameterizedRoot = parameterized.left;
            parameterRowType = parameterized.right;
        } else {
            // This query is an execution of a prepared statement
            parameterizedRoot = routedRoot;
        }

        //
        // Implementation Caching
        if ( isAnalyze ) {
            statement.getProcessingDuration().start( "Implementation Caching" );
        }
        if ( RuntimeConfig.IMPLEMENTATION_CACHING.getBoolean() && statement.getTransaction().getUseCache() && (!routedRoot.kind.belongsTo( Kind.DML ) || RuntimeConfig.IMPLEMENTATION_CACHING_DML.getBoolean() || statement.getDataContext().getParameterValues().size() > 0) ) {
            PreparedResult preparedResult = ImplementationCache.INSTANCE.getIfPresent( parameterizedRoot.rel );
            if ( preparedResult != null ) {
                PolyphenyDbSignature signature = createSignature( preparedResult, routedRoot, resultConvention, executionTimeMonitor );
                if ( isAnalyze ) {
                    statement.getProcessingDuration().stop( "Implementation Caching" );
                }

                //TODO @Cedric this produces an error causing several checks to fail. Please investigate
                //needed for row results

                //final Enumerable enumerable = signature.enumerable( statement.getDataContext() );
                //Iterator<Object> iterator = enumerable.iterator();

                if ( statement.getTransaction().getMonitoringData() != null ) {
                    StatementEvent eventData = statement.getTransaction().getMonitoringData();
                    eventData.setMonitoringType( parameterizedRoot.kind.sql );
                    eventData.setDescription( "Test description: " + signature.statementType.toString() );
                    eventData.setRouted( logicalRoot );
                    eventData.setFieldNames( ImmutableList.copyOf( signature.rowType.getFieldNames() ) );
                    //eventData.setRows( MetaImpl.collect( signature.cursorFactory, iterator, new ArrayList<>() ) );
                    eventData.setAnalyze( isAnalyze );
                    eventData.setSubQuery( isSubquery );
                    //eventData.setDurations( statement.getProcessingDuration().asJson() );
                }

                return signature;
            }
        }

        //
        // Plan Caching
        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Implementation Caching" );
            statement.getProcessingDuration().start( "Plan Caching" );
        }
        RelNode optimalNode;
        if ( RuntimeConfig.QUERY_PLAN_CACHING.getBoolean() && statement.getTransaction().getUseCache() && (!routedRoot.kind.belongsTo( Kind.DML ) || RuntimeConfig.QUERY_PLAN_CACHING_DML.getBoolean() || statement.getDataContext().getParameterValues().size() > 0) ) {
            optimalNode = QueryPlanCache.INSTANCE.getIfPresent( parameterizedRoot.rel );
        } else {
            parameterizedRoot = routedRoot;
            optimalNode = null;
        }

        //
        // Planning & Optimization
        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Plan Caching" );
            statement.getProcessingDuration().start( "Planning & Optimization" );
        }

        if ( optimalNode == null ) {
            optimalNode = optimize( parameterizedRoot, resultConvention );

            // For transformation from DML -> DML, use result of rewrite (e.g. UPDATE -> MERGE). For anything else (e.g. CALL -> SELECT), use original kind.
            //if ( !optimalRoot.kind.belongsTo( Kind.DML ) ) {
            //    optimalRoot = optimalRoot.withKind( sqlNodeOriginal.getKind() );
            //}

            if ( RuntimeConfig.QUERY_PLAN_CACHING.getBoolean() && statement.getTransaction().getUseCache() && (!routedRoot.kind.belongsTo( Kind.DML ) || RuntimeConfig.QUERY_PLAN_CACHING_DML.getBoolean() || statement.getDataContext().getParameterValues().size() > 0) ) {
                QueryPlanCache.INSTANCE.put( parameterizedRoot.rel, optimalNode );
            }
        }

        final RelDataType rowType = parameterizedRoot.rel.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        RelRoot optimalRoot = new RelRoot( optimalNode, rowType, parameterizedRoot.kind, fields, relCollation( parameterizedRoot.rel ) );

        //
        // Implementation
        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Planning & Optimization" );
            statement.getProcessingDuration().start( "Implementation" );
        }

        PreparedResult preparedResult = implement( optimalRoot, parameterRowType );

        // Cache implementation
        if ( RuntimeConfig.IMPLEMENTATION_CACHING.getBoolean() && statement.getTransaction().getUseCache() && (!routedRoot.kind.belongsTo( Kind.DML ) || RuntimeConfig.IMPLEMENTATION_CACHING_DML.getBoolean() || statement.getDataContext().getParameterValues().size() > 0) ) {
            if ( optimalRoot.rel.isImplementationCacheable() ) {
                ImplementationCache.INSTANCE.put( parameterizedRoot.rel, preparedResult );
            } else {
                ImplementationCache.INSTANCE.countUncacheable();
            }
        }

        PolyphenyDbSignature signature = createSignature( preparedResult, optimalRoot, resultConvention, executionTimeMonitor );
        signature.setSchemaType( schemaType );

        if ( isAnalyze ) {
            statement.getProcessingDuration().stop( "Implementation" );
        }

        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Preparing statement ... done. [{}]", stopWatch );
        }

        //TODO @Cedric this produces an error causing several checks to fail. Please investigate
        //needed for row results
        //final Enumerable enumerable = signature.enumerable( statement.getDataContext() );
        //Iterator<Object> iterator = enumerable.iterator();

        TransactionImpl transaction = (TransactionImpl) statement.getTransaction();
        if ( transaction.getMonitoringData() != null ) {
            StatementEvent eventData = transaction.getMonitoringData();
            eventData.setMonitoringType( parameterizedRoot.kind.sql );
            eventData.setDescription( "Test description: " + signature.statementType.toString() );
            eventData.setRouted( logicalRoot );
            eventData.setFieldNames( ImmutableList.copyOf( signature.rowType.getFieldNames() ) );
            //eventData.setRows( MetaImpl.collect( signature.cursorFactory, iterator, new ArrayList<>() ) );
            eventData.setAnalyze( isAnalyze );
            eventData.setSubQuery( isSubquery );
            //eventData.setDurations( statement.getProcessingDuration().asJson() );
        }

        return signature;
    }


    private RelRoot indexUpdate( RelRoot root, Statement statement, RelDataType parameterRowType ) {
        if ( root.kind.belongsTo( Kind.DML ) ) {
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
                            RelRoot scanRoot = RelRoot.of( originalProject, Kind.SELECT );
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


    private RelRoot indexLookup( RelRoot logicalRoot, Statement statement, ExecutionTimeMonitor executionTimeMonitor ) {
        final RelBuilder builder = RelBuilder.create( statement, logicalRoot.rel.getCluster() );
        final RexBuilder rexBuilder = builder.getRexBuilder();
        RelNode newRoot = logicalRoot.rel;
        if ( logicalRoot.kind.belongsTo( Kind.DML ) ) {
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


    private RelRoot route( RelRoot logicalRoot, Statement statement, ExecutionTimeMonitor executionTimeMonitor ) {
        RelRoot routedRoot = statement.getRouter().route( logicalRoot, statement, executionTimeMonitor );
        if ( log.isTraceEnabled() ) {
            log.trace( "Routed query plan: [{}]", RelOptUtil.dumpPlan( "-- Routed Plan", routedRoot.rel, ExplainFormat.TEXT, ExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        if ( statement.getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "Routed Query Plan" ).setLabel( "plans" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Routed Query Plan" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    group,
                    RelOptUtil.dumpPlan( "Routed Query Plan", routedRoot.rel, ExplainFormat.JSON, ExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        }
        return routedRoot;
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


    private PolyphenyDbSignature createSignature( PreparedResult preparedResult, RelRoot optimalRoot, Convention resultConvention, ExecutionTimeMonitor executionTimeMonitor ) {
        final RelDataType jdbcType = makeStruct( optimalRoot.rel.getCluster().getTypeFactory(), optimalRoot.validatedRowType );
        final List<AvaticaParameter> parameters = new ArrayList<>();
        for ( RelDataTypeField field : preparedResult.getParameterRowType().getFieldList() ) {
            RelDataType type = field.getType();
            parameters.add(
                    new AvaticaParameter(
                            false,
                            getPrecision( type ),
                            0, // This is a workaround for a bug in Avatica with Decimals. There is no need to change the scale //getScale( type ),
                            getTypeOrdinal( type ),
                            type.getPolyType().getTypeName(),
                            getClassName( type ),
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
        final List<ColumnMetaData> columns = getColumnMetaDataList(
                statement.getTransaction().getTypeFactory(),
                x,
                makeStruct( statement.getTransaction().getTypeFactory(), x ),
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
                getStatementType( preparedResult ),
                executionTimeMonitor );
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


    private RelCollation relCollation( RelNode node ) {
        return node instanceof Sort
                ? ((Sort) node).collation
                : RelCollations.EMPTY;
    }


    private PreparedResult implement( RelRoot root, RelDataType parameterRowType ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "Physical query plan: [{}]", RelOptUtil.dumpPlan( "-- Physical Plan", root.rel, ExplainFormat.TEXT, ExplainLevel.DIGEST_ATTRIBUTES ) );
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
                    RelOptUtil.dumpPlan( "Physical Query Plan", root.rel, ExplainFormat.JSON, ExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        }

        final RelDataType jdbcType = makeStruct( root.rel.getCluster().getTypeFactory(), root.validatedRowType );
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
                final Conformance conformance = statement.getPrepareContext().config().conformance();

                final Map<String, Object> internalParameters = new LinkedHashMap<>();
                internalParameters.put( "_conformance", conformance );

                bindable = EnumerableInterpretable.toBindable( internalParameters, statement.getPrepareContext().spark(), enumerable, prefer, statement );
                statement.getDataContext().addAll( internalParameters );
            } finally {
                CatalogReader.THREAD_LOCAL.remove();
            }
        }

        RelDataType resultType = root.rel.getRowType();
        boolean isDml = root.kind.belongsTo( Kind.DML );

        return new PreparedResultImpl(
                resultType,
                parameterRowType,
                fieldOrigins,
                root.collation.getFieldCollations().isEmpty()
                        ? ImmutableList.of()
                        : ImmutableList.of( root.collation ),
                root.rel,
                mapTableModOp( isDml, root.kind ),
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


    private StatementType getStatementType( PreparedResult preparedResult ) {
        if ( preparedResult.isDml() ) {
            return StatementType.IS_DML;
        } else {
            return StatementType.SELECT;
        }
    }


    private static RelDataType makeStruct( RelDataTypeFactory typeFactory, RelDataType type ) {
        if ( type.isStruct() ) {
            return type;
        }
        // TODO MV: This "null" might be wrong
        return typeFactory.builder().add( "$0", null, type ).build();
    }


    private static String origin( List<String> origins, int offsetFromEnd ) {
        return origins == null || offsetFromEnd >= origins.size()
                ? null
                : origins.get( origins.size() - 1 - offsetFromEnd );
    }


    private static int getScale( RelDataType type ) {
        return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED
                ? 0
                : type.getScale();
    }


    private static int getPrecision( RelDataType type ) {
        return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
                ? 0
                : type.getPrecision();
    }


    private static String getClassName( RelDataType type ) {
        return Object.class.getName();
    }


    private static int getTypeOrdinal( RelDataType type ) {
        return type.getPolyType().getJdbcOrdinal();
    }


    protected LogicalTableModify.Operation mapTableModOp( boolean isDml, Kind Kind ) {
        if ( !isDml ) {
            return null;
        }
        switch ( Kind ) {
            case INSERT:
                return LogicalTableModify.Operation.INSERT;
            case DELETE:
                return LogicalTableModify.Operation.DELETE;
            case MERGE:
                return LogicalTableModify.Operation.MERGE;
            case UPDATE:
                return LogicalTableModify.Operation.UPDATE;
            default:
                return null;
        }
    }


    private List<ColumnMetaData> getColumnMetaDataList( JavaTypeFactory typeFactory, RelDataType x, RelDataType jdbcType, List<List<String>> originList ) {
        final List<ColumnMetaData> columns = new ArrayList<>();
        for ( Ord<RelDataTypeField> pair : Ord.zip( jdbcType.getFieldList() ) ) {
            final RelDataTypeField field = pair.e;
            final RelDataType type = field.getType();
            final RelDataType fieldType = x.isStruct() ? x.getFieldList().get( pair.i ).getType() : type;
            columns.add( metaData( typeFactory, columns.size(), field.getName(), type, fieldType, originList.get( pair.i ) ) );
        }
        return columns;
    }


    private ColumnMetaData.AvaticaType avaticaType( JavaTypeFactory typeFactory, RelDataType type, RelDataType fieldType ) {
        final String typeName = type.getPolyType().getTypeName();
        if ( type.getComponentType() != null ) {
            final ColumnMetaData.AvaticaType componentType = avaticaType( typeFactory, type.getComponentType(), null );
//            final Type clazz = typeFactory.getJavaClass( type.getComponentType() );
//            final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of( clazz );
            final ColumnMetaData.Rep rep = Rep.ARRAY;
//            assert rep != null;
            return ColumnMetaData.array( componentType, typeName, rep );
        } else {
            int typeOrdinal = getTypeOrdinal( type );
            switch ( typeOrdinal ) {
                case Types.STRUCT:
                    final List<ColumnMetaData> columns = new ArrayList<>();
                    for ( RelDataTypeField field : type.getFieldList() ) {
                        columns.add( metaData( typeFactory, field.getIndex(), field.getName(), field.getType(), null, null ) );
                    }
                    return ColumnMetaData.struct( columns );
                case ExtraPolyTypes.GEOMETRY:
                    typeOrdinal = Types.VARCHAR;
                    // fall through
                default:
                    final Type clazz = typeFactory.getJavaClass( Util.first( fieldType, type ) );
                    final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of( clazz );
                    assert rep != null;
                    return ColumnMetaData.scalar( typeOrdinal, typeName, rep );
            }
        }
    }


    private ColumnMetaData metaData(
            JavaTypeFactory typeFactory,
            int ordinal,
            String fieldName,
            RelDataType type,
            RelDataType fieldType,
            List<String> origins ) {
        final ColumnMetaData.AvaticaType avaticaType = avaticaType( typeFactory, type, fieldType );
        return new ColumnMetaData(
                ordinal,
                false,
                true,
                false,
                false,
                type.isNullable()
                        ? DatabaseMetaData.columnNullable
                        : DatabaseMetaData.columnNoNulls,
                true,
                type.getPrecision(),
                fieldName,
                origin( origins, 0 ),
                origin( origins, 2 ),
                getPrecision( type ),
                0, // This is a workaround for a bug in Avatica with Decimals. There is no need to change the scale //getScale( type ),
                origin( origins, 1 ),
                null,
                avaticaType,
                true,
                false,
                false,
//                avaticaType.columnClassName() );
                (fieldType instanceof ArrayType) ? "java.util.List" : avaticaType.columnClassName() );
    }


    @Override
    public RelRoot expandView( RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath ) {
        return null; // TODO
    }


    @Override
    public void resetCaches() {
        ImplementationCache.INSTANCE.reset();
        QueryPlanCache.INSTANCE.reset();
        statement.getRouter().resetCaches();
    }


    static class RelDeepCopyShuttle extends RelShuttleImpl {

        private RelTraitSet copy( final RelTraitSet other ) {
            return RelTraitSet.createEmpty().merge( other );
        }


        @Override
        public RelNode visit( TableScan scan ) {
            final RelNode node = super.visit( scan );
            return new LogicalTableScan( node.getCluster(), copy( node.getTraitSet() ), node.getTable() );
        }


        @Override
        public RelNode visit( TableFunctionScan scan ) {
            final RelNode node = super.visit( scan );
            return node.copy( copy( node.getTraitSet() ), node.getInputs() );
        }


        @Override
        public RelNode visit( LogicalValues values ) {
            final Values node = (Values) super.visit( values );
            return new LogicalValues( node.getCluster(), copy( node.getTraitSet() ), node.getRowType(), node.getTuples() );
        }


        @Override
        public RelNode visit( LogicalFilter filter ) {
            final LogicalFilter node = (LogicalFilter) super.visit( filter );
            return new LogicalFilter( node.getCluster(), copy( node.getTraitSet() ), node.getInput().accept( this ), node.getCondition(), node.getVariablesSet() );
        }


        @Override
        public RelNode visit( LogicalProject project ) {
            final Project node = (Project) super.visit( project );
            return new LogicalProject( node.getCluster(), copy( node.getTraitSet() ), node.getInput(), node.getProjects(), node.getRowType() );
        }


        @Override
        public RelNode visit( LogicalJoin join ) {
            final RelNode node = super.visit( join );
            return new LogicalJoin( node.getCluster(), copy( node.getTraitSet() ), this.visit( join.getLeft() ), this.visit( join.getRight() ), join.getCondition(), join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(), ImmutableList.copyOf( join.getSystemFieldList() ) );
        }


        @Override
        public RelNode visit( LogicalCorrelate correlate ) {
            final RelNode node = super.visit( correlate );
            return node.copy( copy( node.getTraitSet() ), node.getInputs() );
        }


        @Override
        public RelNode visit( LogicalUnion union ) {
            final RelNode node = super.visit( union );
            return node.copy( copy( node.getTraitSet() ), node.getInputs() );
        }


        @Override
        public RelNode visit( LogicalIntersect intersect ) {
            final RelNode node = super.visit( intersect );
            return node.copy( copy( node.getTraitSet() ), node.getInputs() );
        }


        @Override
        public RelNode visit( LogicalMinus minus ) {
            final RelNode node = super.visit( minus );
            return node.copy( copy( node.getTraitSet() ), node.getInputs() );
        }


        @Override
        public RelNode visit( LogicalAggregate aggregate ) {
            final RelNode node = super.visit( aggregate );
            return new LogicalAggregate( node.getCluster(), copy( node.getTraitSet() ), visit( aggregate.getInput() ), aggregate.indicator, aggregate.getGroupSet(), aggregate.groupSets, aggregate.getAggCallList() );
        }


        @Override
        public RelNode visit( LogicalMatch match ) {
            final RelNode node = super.visit( match );
            return node.copy( copy( node.getTraitSet() ), node.getInputs() );
        }


        @Override
        public RelNode visit( LogicalSort sort ) {
            final RelNode node = super.visit( sort );
            return node.copy( copy( node.getTraitSet() ), node.getInputs() );
        }


        @Override
        public RelNode visit( LogicalExchange exchange ) {
            final RelNode node = super.visit( exchange );
            return node.copy( copy( node.getTraitSet() ), node.getInputs() );
        }


        @Override
        public RelNode visit( LogicalConditionalExecute lce ) {
            return new LogicalConditionalExecute( lce.getCluster(), copy( lce.getTraitSet() ), visit( lce.getLeft() ), visit( lce.getRight() ), lce.getCondition(), lce.getExceptionClass(), lce.getExceptionMessage() );
        }


        @Override
        public RelNode visit( RelNode other ) {
            final RelNode node = super.visit( other );
            return node.copy( copy( node.getTraitSet() ), node.getInputs() );
        }

    }

}
