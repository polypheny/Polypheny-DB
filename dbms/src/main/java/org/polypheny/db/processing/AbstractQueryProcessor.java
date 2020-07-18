/*
 * Copyright 2019-2020 The Polypheny Project
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
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Ord;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.adapter.DeferredIndexUpdate;
import org.polypheny.db.adapter.Index;
import org.polypheny.db.adapter.IndexManager;
import org.polypheny.db.adapter.enumerable.EnumerableCalc;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableInterpretable;
import org.polypheny.db.adapter.enumerable.EnumerableRel;
import org.polypheny.db.adapter.enumerable.EnumerableRel.Prefer;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.interpreter.Interpreters;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptTable.ViewExpander;
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
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.RelShuttle;
import org.polypheny.db.rel.RelShuttleImpl;
import org.polypheny.db.rel.core.ConditionalExecute.Condition;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.logical.LogicalConditionalExecute;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.Typed;
import org.polypheny.db.sql.SqlExplainFormat;
import org.polypheny.db.sql.SqlExplainLevel;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
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
import org.polypheny.db.type.ExtraPolyTypes;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


@Slf4j
public abstract class AbstractQueryProcessor implements QueryProcessor, ViewExpander {

    private final Statement statement;

    protected static final boolean ENABLE_BINDABLE = false;
    protected static final boolean ENABLE_COLLATION_TRAIT = true;
    protected static final boolean ENABLE_ENUMERABLE = true;
    protected static final boolean CONSTANT_REDUCTION = false;
    protected static final boolean ENABLE_STREAM = true;


    protected AbstractQueryProcessor( Statement statement ) {
        this.statement = statement;
    }


    @Override
    public PolyphenyDbSignature prepareQuery( RelRoot logicalRoot ) {
        return prepareQuery(
                logicalRoot,
                logicalRoot.rel.getCluster().getTypeFactory().builder().build(),
                null );
    }


    @Override
    public PolyphenyDbSignature prepareQuery( RelRoot logicalRoot, RelDataType parameterRowType, Map<String, Object> values ) {
        // If this is a prepared statement, values is != null
        if ( values != null ) {
            statement.getDataContext().addAll( values );
        }

        final StopWatch stopWatch = new StopWatch();

        if ( log.isDebugEnabled() ) {
            log.debug( "Preparing statement  ..." );
        }
        stopWatch.start();

        ExecutionTimeMonitor executionTimeMonitor = new ExecutionTimeMonitor();

        final Convention resultConvention =
                ENABLE_BINDABLE
                        ? BindableConvention.INSTANCE
                        : EnumerableConvention.INSTANCE;

        // Locking
        if ( statement.getTransaction().isAnalyze() ) {
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

        // Constraint Enforcement Rewrite
        if ( statement.getTransaction().isAnalyze() ) {
            statement.getDuration().stop( "Locking" );
            statement.getDuration().start( "Index Update" );
        }
        RelRoot indexUpdateRoot = indexUpdate( logicalRoot, statement );

        // Constraint Enforcement Rewrite
        if ( statement.getTransaction().isAnalyze() ) {
            statement.getDuration().stop( "Index Update" );
            statement.getDuration().start( "Constraint Enforcement" );
        }
        RelRoot constraintsRoot = enforceConstraints( indexUpdateRoot, statement );

        // Index Lookup Rewrite
        if ( statement.getTransaction().isAnalyze() ) {
            statement.getDuration().stop( "Constraint Enforcement" );
            statement.getDuration().start( "Index Lookup Rewrite" );
        }
        RelRoot indexLookupRoot = indexLookup( constraintsRoot, statement, executionTimeMonitor );

        // Route
        if ( statement.getTransaction().isAnalyze() ) {
            statement.getDuration().stop( "Index Lookup Rewrite" );
            statement.getDuration().start( "Routing" );
        }
        RelRoot routedRoot = route( indexLookupRoot, statement, executionTimeMonitor );

        RelStructuredTypeFlattener typeFlattener = new RelStructuredTypeFlattener(
                RelBuilder.create( statement, routedRoot.rel.getCluster() ),
                routedRoot.rel.getCluster().getRexBuilder(),
                ViewExpanders.toRelContext( this, routedRoot.rel.getCluster() ),
                true );
        routedRoot = routedRoot.withRel( typeFlattener.rewrite( routedRoot.rel ) );

        //
        // Implementation Caching
        if ( statement.getTransaction().isAnalyze() ) {
            statement.getDuration().stop( "Routing" );
            statement.getDuration().start( "Implementation Caching" );
        }
        RelRoot parameterizedRoot = null;
        if ( RuntimeConfig.IMPLEMENTATION_CACHING.getBoolean() && (!routedRoot.kind.belongsTo( SqlKind.DML ) || RuntimeConfig.IMPLEMENTATION_CACHING_DML.getBoolean() || values != null) ) {
            if ( values == null ) {
                Pair<RelRoot, RelDataType> parameterized = parameterize( routedRoot, parameterRowType );
                parameterizedRoot = parameterized.left;
                parameterRowType = parameterized.right;
            } else {
                // This query is an execution of a prepared statement
                parameterizedRoot = routedRoot;
            }
            PreparedResult preparedResult = ImplementationCache.INSTANCE.getIfPresent( parameterizedRoot.rel );
            if ( preparedResult != null ) {
                PolyphenyDbSignature signature = createSignature( preparedResult, routedRoot, resultConvention, executionTimeMonitor );
                if ( statement.getTransaction().isAnalyze() ) {
                    statement.getDuration().stop( "Implementation Caching" );
                }
                return signature;
            }
        }

        //
        // Plan Caching
        if ( statement.getTransaction().isAnalyze() ) {
            statement.getDuration().stop( "Implementation Caching" );
            statement.getDuration().start( "Plan Caching" );
        }
        RelNode optimalNode;
        if ( RuntimeConfig.QUERY_PLAN_CACHING.getBoolean() && (!routedRoot.kind.belongsTo( SqlKind.DML ) || RuntimeConfig.QUERY_PLAN_CACHING_DML.getBoolean() || values != null) ) {
            if ( parameterizedRoot == null ) {
                if ( values == null ) {
                    Pair<RelRoot, RelDataType> parameterized = parameterize( routedRoot, parameterRowType );
                    parameterizedRoot = parameterized.left;
                    parameterRowType = parameterized.right;
                } else {
                    // This query is an execution of a prepared statement
                    parameterizedRoot = routedRoot;
                }
            }
            optimalNode = QueryPlanCache.INSTANCE.getIfPresent( parameterizedRoot.rel );
        } else {
            parameterizedRoot = routedRoot;
            optimalNode = null;
        }

        //
        // Planning & Optimization
        if ( statement.getTransaction().isAnalyze() ) {
            statement.getDuration().stop( "Plan Caching" );
            statement.getDuration().start( "Planning & Optimization" );
        }

        if ( optimalNode == null ) {
            optimalNode = optimize( parameterizedRoot, resultConvention );

            // For transformation from DML -> DML, use result of rewrite (e.g. UPDATE -> MERGE). For anything else (e.g. CALL -> SELECT), use original kind.
            //if ( !optimalRoot.kind.belongsTo( SqlKind.DML ) ) {
            //    optimalRoot = optimalRoot.withKind( sqlNodeOriginal.getKind() );
            //}

            if ( RuntimeConfig.QUERY_PLAN_CACHING.getBoolean() && (!routedRoot.kind.belongsTo( SqlKind.DML ) || RuntimeConfig.QUERY_PLAN_CACHING_DML.getBoolean() || values != null) ) {
                QueryPlanCache.INSTANCE.put( parameterizedRoot.rel, optimalNode );
            }
        }

        final RelDataType rowType = parameterizedRoot.rel.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        RelRoot optimalRoot = new RelRoot( optimalNode, rowType, parameterizedRoot.kind, fields, relCollation( parameterizedRoot.rel ) );

        //
        // Implementation
        if ( statement.getTransaction().isAnalyze() ) {
            statement.getDuration().stop( "Planning & Optimization" );
            statement.getDuration().start( "Implementation" );
        }

        PreparedResult preparedResult = implement( optimalRoot, parameterRowType );

        // Cache implementation
        if ( RuntimeConfig.IMPLEMENTATION_CACHING.getBoolean() && (!routedRoot.kind.belongsTo( SqlKind.DML ) || RuntimeConfig.IMPLEMENTATION_CACHING_DML.getBoolean() || values != null) ) {
            if ( optimalRoot.rel.isImplementationCacheable() ) {
                ImplementationCache.INSTANCE.put( parameterizedRoot.rel, preparedResult );
            } else {
                ImplementationCache.INSTANCE.countUncacheable();
            }
        }

        PolyphenyDbSignature signature = createSignature( preparedResult, optimalRoot, resultConvention, executionTimeMonitor );

        if ( statement.getTransaction().isAnalyze() ) {
            statement.getDuration().stop( "Implementation" );
        }

        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Preparing statement ... done. [{}]", stopWatch );
        }

        return signature;
    }




    private RelRoot indexUpdate( RelRoot root, Statement statement ) {
        if ( root.kind.belongsTo( SqlKind.DML ) ) {
            final RelShuttle shuttle = new RelShuttleImpl() {

                @Override
                public RelNode visit( RelNode node ) {
                    RexBuilder rexBuilder = new RexBuilder( transaction.getTypeFactory() );
                    if ( node instanceof LogicalTableModify ) {
                        final Catalog catalog = Catalog.getInstance();
                        final CatalogSchema schema = transaction.getDefaultSchema();
                        final LogicalTableModify ltm = (LogicalTableModify) node;
                        final CatalogTable table;
                        try {
                            table = catalog.getTable( schema.id, ltm.getTable().getQualifiedName().get( 0 ) );
                        } catch ( UnknownTableException | GenericCatalogException e ) {
                            // This really should not happen
                            log.error( String.format( "Table not found: %s", ltm.getTable().getQualifiedName().get( 0 ) ), e );
                            throw new RuntimeException( e );
                        }
                        final List<Index> indices = IndexManager.getInstance().getIndices( schema, table );

                        if ( ltm.isInsert() ) {
                            final LogicalValues values = (LogicalValues) ltm.getInput( 0 );
                            for ( final Index index : indices ) {
                                final Set<Pair<List<Object>, List<Object>>> tuplesToInsert = new HashSet<>( values.tuples.size() );
                                for ( final ImmutableList<RexLiteral> row : values.getTuples() ) {
                                    final List<Object> rowValues = new ArrayList<>();
                                    final List<Object> targetRowValues = new ArrayList<>();
                                    for ( final String column : index.getColumns() ) {
                                        final RexLiteral fieldValue = row.get(
                                                values.getRowType().getField( column, false, false ).getIndex()
                                        );
                                        rowValues.add( fieldValue.getValue() );
                                    }
                                    for ( final String column : index.getTargetColumns() ) {
                                        final RexLiteral fieldValue = row.get(
                                                values.getRowType().getField( column, false, false ).getIndex()
                                        );
                                        targetRowValues.add( fieldValue.getValue() );
                                    }
                                    tuplesToInsert.add( new Pair<>( rowValues, targetRowValues ) );
                                }
                                transaction.deferIndexUpdate( DeferredIndexUpdate.createInsert( index.getId(), tuplesToInsert ) );
                            }
                        } else if ( ltm.isDelete() || ltm.isUpdate() ) {
                            final Map<String, Integer> nameMap = new HashMap<>();
                            final Map<String, Integer> newValueMap = new HashMap<>();
                            final LogicalProject originalProject = ((LogicalProject) ltm.getInput());

                            for ( int i = 0; i < originalProject.getNamedProjects().size(); ++i ) {
                                final Pair<RexNode, String> np = originalProject.getNamedProjects().get( i );
                                nameMap.put( np.right, i );
                                if ( ltm.isUpdate() ) {
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
                                }
                            }
                            // Prepare subquery
                            final ExecutionTimeMonitor executionTimeMonitor = new ExecutionTimeMonitor();
                            final Convention resultConvention =
                                    ENABLE_BINDABLE
                                            ? BindableConvention.INSTANCE
                                            : EnumerableConvention.INSTANCE;
                            final RelRoot root = RelRoot.of( originalProject, SqlKind.SELECT );
                            final RelRoot routed = route( root, transaction, executionTimeMonitor );
                            RelStructuredTypeFlattener typeFlattener = new RelStructuredTypeFlattener(
                                    RelBuilder.create( transaction, routed.rel.getCluster() ),
                                    rexBuilder,
                                    ViewExpanders.toRelContext( AbstractQueryProcessor.this, routed.rel.getCluster() ),
                                    true );
                            final RelRoot flattenedRoot = routed.withRel( typeFlattener.rewrite( routed.rel ) );
                            final RelNode optimized = optimize( flattenedRoot, resultConvention );
                            RelRoot optimalRoot = new RelRoot( optimized, flattenedRoot.validatedRowType, flattenedRoot.kind, flattenedRoot.fields, relCollation( flattenedRoot.rel ) );
                            // Implement and execute subquery
                            final RelDataType jdbcType = makeStruct( rexBuilder.getTypeFactory(), root.validatedRowType );
                            final PreparedResult pr = implement( optimalRoot, jdbcType );
                            PolyphenyDbSignature scanSig = createSignature( pr, optimalRoot, resultConvention, executionTimeMonitor );
                            final Iterable<Object> enumerable = scanSig.enumerable( transaction.getDataContext() );
                            final Iterator<Object> iterator = enumerable.iterator();
                            final List<List<Object>> rows = MetaImpl.collect( scanSig.cursorFactory, iterator, new ArrayList<>() );
                            // Schedule the index deletions
                            for ( final Index index : indices ) {
                                if ( ltm.isUpdate() ) {
                                    // Index not affected by this update, skip
                                    if ( index.getColumns().stream().noneMatch( ltm.getUpdateColumnList()::contains ) ) {
                                        continue;
                                    }
                                }
                                final Set<List<Object>> rowsToDelete = new HashSet<>( rows.size() );
                                for ( List<Object> row : rows ) {
                                    final List<Object> rowProjection = new ArrayList<>( index.getColumns().size() );
                                    for ( final String column : index.getColumns() ) {
                                        rowProjection.add( row.get( nameMap.get( column ) ) );
                                    }
                                    rowsToDelete.add( rowProjection );
                                }
                                transaction.deferIndexUpdate( DeferredIndexUpdate.createDelete( index.getId(), rowsToDelete ) );
                            }
                            // TODO(s3lph): These indices should rather be updated when enforcing foreign key constraints by propagating deletes/updates to dependant tables
//                            for (final Index index : reverseIndices) {
//                                final Set<List<Object>> rowsToDelete = new HashSet<>( rows.size() );
//                                for ( List<Object> row : rows ) {
//                                    final List<Object> rowProjection = new ArrayList<>( index.getTargetColumns().size() );
//                                    for ( final String column : index.getTargetColumns() ) {
//                                        rowProjection.add( row.get( nameMap.get( column ) ) );
//                                    }
//                                    rowsToDelete.add( rowProjection );
//                                }
//                                transaction.deferIndexUpdate( DeferredIndexUpdate.createReverseDelete( index.getId(), rowsToDelete ) );
//                            }
                            //Schedule the index insertions for UPDATE operations
                            if ( ltm.isUpdate() ) {
                                for ( final Index index : indices ) {
                                    // Index not affected by this update, skip
                                    if ( index.getColumns().stream().noneMatch( ltm.getUpdateColumnList()::contains ) ) {
                                        continue;
                                    }
                                    final Set<Pair<List<Object>, List<Object>>> rowsToReinsert = new HashSet<>( rows.size() );
                                    for ( List<Object> row : rows ) {
                                        final List<Object> rowProjection = new ArrayList<>( index.getColumns().size() );
                                        final List<Object> targetRowProjection = new ArrayList<>( index.getTargetColumns().size() );
                                        for ( final String column : index.getColumns() ) {
                                            rowProjection.add( row.get( newValueMap.get( column ) ) );
                                        }
                                        for ( final String column : index.getTargetColumns() ) {
                                            targetRowProjection.add( row.get( nameMap.get( column ) ) );
                                        }
                                        rowsToReinsert.add( new Pair<>( rowProjection, targetRowProjection ) );
                                    }
                                    transaction.deferIndexUpdate( DeferredIndexUpdate.createInsert( index.getId(), rowsToReinsert ) );
                                }
                            }
                        }

                    }
                    return super.visit( node );
                }

            };
            final RelNode newRoot = shuttle.visit( root.rel );
            return new RelRoot(
                    newRoot,
                    root.validatedRowType,
                    root.kind,
                    root.fields,
                    root.collation );

        }
        return root;
    }


    private RelRoot enforceConstraints( RelRoot logicalRoot, Statement statement ) {
        if ( !logicalRoot.kind.belongsTo( SqlKind.DML ) ) {
            return logicalRoot;
        }
        if ( !(logicalRoot.rel instanceof TableModify) || !((TableModify) logicalRoot.rel).isInsert() ) {
            return logicalRoot;
        }
        final TableModify root = (TableModify) logicalRoot.rel;

        final Values values = (Values) root.getInput();
        final LogicalTableScan scan = LogicalTableScan.create( root.getCluster(), root.getTable() );
        final RexBuilder builder = root.getCluster().getRexBuilder();

        final Catalog catalog = Catalog.getInstance();
        final CatalogSchema schema = transaction.getDefaultSchema();
        final CatalogTable table;
        final List<CatalogConstraint> constraints;

        if ( root.isInsert() ) {
            try {
                table = catalog.getTable( schema.id, root.getTable().getQualifiedName().get( 0 ) );
                constraints = new ArrayList<>( catalog.getConstraints( table.id ) );
                // Turn primary key into an artificial unique constraint
                if ( table.primaryKey != null ) {
                    CatalogPrimaryKey pk = catalog.getPrimaryKey( table.primaryKey );
                    final CatalogConstraint pkc = new CatalogConstraint(
                            0L, pk.id, ConstraintType.UNIQUE, "PRIMARY KEY", pk );
                    constraints.add( pkc );
                }
            } catch ( UnknownTableException | GenericCatalogException | UnknownKeyException e ) {
                e.printStackTrace();
                return logicalRoot;
            }

            RelNode lceRoot = root;
            for ( final CatalogConstraint constraint : constraints ) {
                if ( constraint.type != ConstraintType.UNIQUE ) {
                    log.warn( "Unknown constraint type: " + constraint.type );
                    continue;
                }

                RexNode condition = builder.makeLiteral( false );
                final Set<List<Object>> uniqueSet = new HashSet<>();
                for ( final ImmutableList<RexLiteral> row : values.getTuples() ) {
                    RexNode rowcheck = builder.makeLiteral( true );
                    final List<Object> rowValues = new ArrayList<>();
                    for ( final String column : constraint.key.getColumnNames() ) {
                        final RexLiteral fieldValue = row.get(
                                values.getRowType().getField( column, false, false ).getIndex()
                        );
                        rowValues.add( fieldValue.getValue() );
                        RexNode comparison = builder.makeCall(
                                SqlStdOperatorTable.EQUALS,
                                builder.makeFieldAccess(
                                        builder.makeRangeReference( scan ), column, false
                                ),
                                fieldValue
                        );
                        rowcheck = builder.makeCall( SqlStdOperatorTable.AND, rowcheck, comparison );
                    }
                    uniqueSet.add( rowValues );
                    condition = builder.makeCall( SqlStdOperatorTable.OR, condition, rowcheck );
                }
                // Validate values in the insert set for uniqueness among each other
                if ( uniqueSet.size() != values.getTuples().size() ) {
                    throw new RuntimeException( "UNIQUE constraint violated" );
                }
                final LogicalFilter filter = LogicalFilter.create( scan, condition );
                final LogicalConditionalExecute lce = LogicalConditionalExecute.create( filter, lceRoot, Condition.EQUAL_TO_ZERO );
                lce.setCatalogSchema( schema );
                lce.setCatalogTable( table );
                lce.setCatalogColumns( constraint.key.getColumnNames() );
                lce.setValues( uniqueSet );
                lceRoot = lce;
            }
            return new RelRoot( lceRoot, logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
        }

        return logicalRoot;
    }


    private RelRoot indexLookup( RelRoot logicalRoot, Statement statement, ExecutionTimeMonitor executionTimeMonitor ) {
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
                            final Condition c = index.containsAny( lce.getValues() ) ? Condition.FALSE : Condition.TRUE;
                            return LogicalConditionalExecute.create( visited.getLeft(), visited.getRight(), c );
                        }
                    }
                    return super.visit( node );
                }

            };
            final RelNode newRoot = shuttle.visit( logicalRoot.rel );
            return new RelRoot(
                    newRoot,
                    logicalRoot.validatedRowType,
                    logicalRoot.kind,
                    logicalRoot.fields,
                    logicalRoot.collation );

        }
        return new RelRoot(
                logicalRoot.rel,
                logicalRoot.validatedRowType,
                logicalRoot.kind,
                logicalRoot.fields,
                logicalRoot.collation );
    }


    private RelRoot route( RelRoot logicalRoot, Statement statement, ExecutionTimeMonitor executionTimeMonitor ) {
        RelRoot routedRoot = statement.getRouter().route( logicalRoot, statement, executionTimeMonitor );
        if ( log.isTraceEnabled() ) {
            log.trace( "Routed query plan: [{}]", RelOptUtil.dumpPlan( "-- Routed Plan", routedRoot.rel, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
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
                    RelOptUtil.dumpPlan( "Routed Query Plan", routedRoot.rel, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
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
        statement.getDataContext().addAll( queryParameterizer.getValues() );

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
                            getScale( type ),
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


    protected LogicalTableModify.Operation mapTableModOp( boolean isDml, SqlKind sqlKind ) {
        if ( !isDml ) {
            return null;
        }
        switch ( sqlKind ) {
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
            final Type clazz = typeFactory.getJavaClass( type.getComponentType() );
            final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of( clazz );
            assert rep != null;
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
                getScale( type ),
                origin( origins, 1 ),
                null,
                avaticaType,
                true,
                false,
                false,
                avaticaType.columnClassName() );
    }


    @Override
    public RelRoot expandView( RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath ) {
        return null; // TODO
    }


    private static class RelCloneShuttle extends RelShuttleImpl {

        protected <T extends RelNode> T visitChild( T parent, int i, RelNode child ) {
            stack.push( parent );
            try {
                RelNode child2 = child.accept( this );
                final List<RelNode> newInputs = new ArrayList<>( parent.getInputs() );
                newInputs.set( i, child2 );
                //noinspection unchecked
                return (T) parent.copy( parent.getTraitSet(), newInputs );
            } finally {
                stack.pop();
            }
        }

    }


    @Override
    public void resetCaches() {
        ImplementationCache.INSTANCE.reset();
        QueryPlanCache.INSTANCE.reset();
    }
}
