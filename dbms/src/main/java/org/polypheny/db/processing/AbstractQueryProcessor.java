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
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Ord;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.adapter.index.Index;
import org.polypheny.db.adapter.index.IndexManager;
import org.polypheny.db.adapter.enumerable.EnumerableCalc;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.EnumerableInterpretable;
import org.polypheny.db.adapter.enumerable.EnumerableRel;
import org.polypheny.db.adapter.enumerable.EnumerableRel.Prefer;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
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
import org.polypheny.db.plan.RelOptSchema;
import org.polypheny.db.plan.RelOptTable;
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
import org.polypheny.db.rel.core.ConditionalExecute.Condition;
import org.polypheny.db.rel.core.JoinRelType;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.core.TableFunctionScan;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.exceptions.ConstraintViolationException;
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
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.Typed;
import org.polypheny.db.sql.SqlExplainFormat;
import org.polypheny.db.sql.SqlExplainLevel;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.fun.SqlCountAggFunction;
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
        return prepareQuery( logicalRoot, parameterRowType, values, false );
    }

    private PolyphenyDbSignature prepareQuery( RelRoot logicalRoot, RelDataType parameterRowType, Map<String, Object> values, boolean isSubquery ) {
        boolean isAnalyze = statement.getTransaction.isAnalyze() && !isSubquery;
        boolean lock = !isSubquery;

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

        if (lock) {
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

            if ( isAnalyze ) {
                statement.getDuration().stop( "Locking" );
            }
        }

        // Constraint Enforcement Rewrite
        if ( isAnalyze ) {
            statement.getDuration().stop( "Locking" );
            statement.getDuration().start( "Index Update" );
        }
        IndexManager.getInstance().barrier( statement.getTransaction().getXid() );
        RelRoot indexUpdateRoot = indexUpdate( logicalRoot, statement, parameterRowType, values );
//        RelRoot indexUpdateRoot = logicalRoot;

        // Constraint Enforcement Rewrite
        if ( isAnalyze ) {
            statement.getDuration().stop( "Index Update" );
            statement.getDuration().start( "Constraint Enforcement" );
        }
        RelRoot constraintsRoot = enforceConstraints( indexUpdateRoot, statement );

        // Index Lookup Rewrite
        if ( isAnalyze ) {
            statement.getDuration().stop( "Constraint Enforcement" );
            statement.getDuration().start( "Index Lookup Rewrite" );
        }
        RelRoot indexLookupRoot = indexLookup( constraintsRoot, statement, executionTimeMonitor );
//        RelRoot indexLookupRoot = constraintsRoot;

        // Route
        if ( isAnalyze ) {
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
        if ( isAnalyze ) {
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
                if ( isAnalyze ) {
                    statement.getDuration().stop( "Implementation Caching" );
                }
                return signature;
            }

        }

        //
        // Plan Caching
        if ( isAnalyze ) {
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
        if ( isAnalyze ) {
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
        if ( isAnalyze ) {
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

        if ( isAnalyze ) {
            statement.getDuration().stop( "Implementation" );
        }

        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Preparing statement ... done. [{}]", stopWatch );
        }

        return signature;
    }




    private RelRoot indexUpdate( RelRoot root, Statement statement, RelDataType parameterRowType, Map<String, Object> values ) {
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

                        if ( ltm.isInsert() && ltm.getInput() instanceof Values ) {
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
                                        rowValues.add( fieldValue.getValue2() );
                                    }
                                    for ( final String column : index.getTargetColumns() ) {
                                        final RexLiteral fieldValue = row.get(
                                                values.getRowType().getField( column, false, false ).getIndex()
                                        );
                                        targetRowValues.add( fieldValue.getValue2() );
                                    }
                                    tuplesToInsert.add( new Pair<>( rowValues, targetRowValues ) );
                                }
                                index.insertAll( statement.getTransaction().getXid(), tuplesToInsert );
                            }
                        } else if ( ltm.isDelete() || ltm.isUpdate() || ( ltm.isInsert() && !( ltm.getInput() instanceof Values ) ) ) {
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
                            RelRoot scanRoot = RelRoot.of( originalProject, SqlKind.SELECT );
                            final PolyphenyDbSignature scanSig = prepareQuery( scanRoot, parameterRowType, values, true );
                            final Iterable<Object> enumerable = scanSig.enumerable( statement.getDataContext() );
                            final Iterator<Object> iterator = enumerable.iterator();
                            final List<List<Object>> rows = MetaImpl.collect( scanSig.cursorFactory, iterator, new ArrayList<>() );
                            // Schedule the index deletions
                            if ( !ltm.isInsert()) {
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
                                        rowsToDelete.add( new Pair<>( rowProjection, targetRowProjection) );
                                    }
                                    index.deleteAllPrimary( statement.getTransaction().getXid(), rowsToDelete );
                                }
                            }
                            //Schedule the index insertions for UPDATE operations
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
        if ( !(logicalRoot.rel instanceof TableModify) ) {
            return logicalRoot;
        }
        final TableModify root = (TableModify) logicalRoot.rel;

        final Catalog catalog = Catalog.getInstance();
        final CatalogSchema schema = statement.getTransaction().getDefaultSchema();
        final CatalogTable table;
        final List<CatalogConstraint> constraints;
        final List<CatalogForeignKey> foreignKeys;
        final List<CatalogForeignKey> exportedKeys;
        try {
            table = catalog.getTable( schema.id, root.getTable().getQualifiedName().get( 0 ) );
            constraints = new ArrayList<>( Catalog.getInstance().getConstraints( table.id ) );
            foreignKeys = Catalog.getInstance().getForeignKeys( table.id );
            exportedKeys = Catalog.getInstance().getExportedKeys( table.id );
            // Turn primary key into an artificial unique constraint
            if ( table.primaryKey != null ) {
                CatalogPrimaryKey pk = Catalog.getInstance().getPrimaryKey( table.primaryKey );
                final CatalogConstraint pkc = new CatalogConstraint(
                        0L, pk.id, ConstraintType.UNIQUE, "PRIMARY KEY", pk );
                constraints.add( pkc );
            }
        } catch ( UnknownTableException | GenericCatalogException | UnknownKeyException e ) {
            e.printStackTrace();
            return logicalRoot;
        }

        RelNode lceRoot = root;

        //
        //  Enforce UNIQUE constraints in INSERT operations
        //
        if ( root.isInsert() ) {
            RelBuilder builder = RelBuilder.create( transaction );
            final RelNode input = root.getInput();
            final RexBuilder rexBuilder = root.getCluster().getRexBuilder();
            for ( final CatalogConstraint constraint : constraints ) {
                if ( constraint.type != ConstraintType.UNIQUE ) {
                    log.warn( "Unknown constraint type: " + constraint.type );
                    continue;
                }
                // Enforce uniqueness between the already existing values and the new values
                final RelNode scan = LogicalTableScan.create( root.getCluster(), root.getTable() );
                RexNode joinCondition = rexBuilder.makeLiteral( true );
                builder.push( input );
                builder.project( constraint.key.getColumnNames().stream().map( builder::field ).collect( Collectors.toList() ) );
                builder.push( scan );
                builder.project( constraint.key.getColumnNames().stream().map( builder::field ).collect( Collectors.toList() ) );
                for ( final String column : constraint.key.getColumnNames() ) {
                    RexNode joinComparison = rexBuilder.makeCall(
                            SqlStdOperatorTable.EQUALS,
                            builder.field( 2, 1, column ),
                            builder.field( 2, 0, column )
                    );
                    joinCondition = rexBuilder.makeCall( SqlStdOperatorTable.AND, joinCondition, joinComparison );
                }
                final RelNode join = builder.join( JoinRelType.LEFT, joinCondition ).build();
                final RelNode check = LogicalFilter.create( join, rexBuilder.makeCall( SqlStdOperatorTable.IS_NOT_NULL, rexBuilder.makeInputRef( join, join.getRowType().getFieldCount() - 1 ) ) );
                final LogicalConditionalExecute lce = LogicalConditionalExecute.create( check, lceRoot, Condition.EQUAL_TO_ZERO,
                        ConstraintViolationException.class,
                        String.format( "Insert violates unique constraint `%s`.`%s`", table.name, constraint.name ) );
                lce.setCheckDescription( String.format( "Enforcement of unique constraint `%s`.`%s`", table.name, constraint.name ) );
                lceRoot = lce;
                // Enforce uniqueness within the values to insert
                //noinspection StatementWithEmptyBody
                if ( input instanceof Values && ((Values) input).getTuples().size() <= 1 ||
                        (input instanceof Project && input.getInput( 0 ) instanceof Values && ((Values) input.getInput( 0 )).getTuples().size() <= 1 ) ) {
                    // no need to check, only one tuple in set
                } else if ( input instanceof Values ) {
                    // If the input is a Values node, check uniqueness right away, as not all stores can implement this check
                    // (And anyway, pushing this down to stores seems rather inefficient)
                    final Values values = (Values) input;
                    final List<? extends List<RexLiteral>> tuples = values.getTuples();
                    final Set<List<RexLiteral>> uniqueSet = new HashSet<>( tuples.size() );
                    final Map<String, Integer> columnMap = new HashMap<>( constraint.key.columnIds.size() );
                    for (final String columnName : constraint.key.getColumnNames()) {
                        int i = values.getRowType().getField( columnName, true, false ).getIndex();
                        columnMap.put( columnName, i );
                    }
                    for ( final List<RexLiteral> tuple : tuples ) {
                        List<RexLiteral> projection = new ArrayList<>( constraint.key.columnIds.size() );
                        for ( final String columnName : constraint.key.getColumnNames() ) {
                            projection.add( tuple.get( columnMap.get( columnName ) ) );
                        }
                        uniqueSet.add( projection );
                    }
                    if ( uniqueSet.size() != tuples.size() ) {
                        throw new ConstraintViolationException( String.format( "Insert violates unique constraint `%s`.`%s`", table.name, constraint.name ) );
                    }
                } else {
                    builder.clear();
                    builder.push( input );
                    builder.aggregate( builder.groupKey( constraint.key.getColumnNames().stream().map( builder::field ).collect( Collectors.toList() ) ), builder.aggregateCall( new SqlCountAggFunction( "count" ) ).as( "count" ) );
                    builder.filter( builder.call( SqlStdOperatorTable.GREATER_THAN, builder.field( "count" ), builder.literal( 1 ) ) );
                    final RelNode innerCheck = builder.build();
                    final LogicalConditionalExecute ilce = LogicalConditionalExecute.create( innerCheck, lceRoot, Condition.EQUAL_TO_ZERO, ConstraintViolationException.class,
                            String.format( "Insert violates unique constraint `%s`.`%s`", table.name, constraint.name ) );
                    ilce.setCheckDescription( String.format( "Source-internal enforcement of unique constraint `%s`.`%s`", table.name, constraint.name ) );
                    lceRoot = ilce;
                }
            }
        }

        //
        //  Enforce FOREIGN KEY constraints in INSERT operations
        //
        if ( root.isInsert() ) {
            RelBuilder builder = RelBuilder.create( statement );
            final RelNode input = root.getInput();
            final RexBuilder rexBuilder = root.getCluster().getRexBuilder();
            for ( final CatalogForeignKey foreignKey : foreignKeys ) {
                final RelOptSchema relOptSchema = root.getCatalogReader();
                final RelOptTable relOptTable = relOptSchema.getTableForMember( Collections.singletonList( foreignKey.getReferencedKeyTableName() ) );
                final LogicalTableScan scan = LogicalTableScan.create( root.getCluster(), relOptTable );
                RexNode joinCondition = rexBuilder.makeLiteral( true );
                builder.push( input );
                builder.project( foreignKey.getColumnNames().stream().map( builder::field ).collect( Collectors.toList()) );
                builder.push( scan );
                builder.project( foreignKey.getReferencedKeyColumnNames().stream().map( builder::field ).collect( Collectors.toList()) );
                for ( int i = 0; i < foreignKey.getColumnNames().size(); ++i ) {
                    final String column = foreignKey.getColumnNames().get( i );
                    final String referencedColumn = foreignKey.getReferencedKeyColumnNames().get( i );
                    RexNode joinComparison = rexBuilder.makeCall(
                            SqlStdOperatorTable.EQUALS,
                            builder.field( 2, 1, referencedColumn ),
                            builder.field( 2, 0, column )
                    );
                    joinCondition = rexBuilder.makeCall( SqlStdOperatorTable.AND, joinCondition, joinComparison );
                }

                final RelNode join = builder.join( JoinRelType.LEFT, joinCondition ).build();
                final RelNode check = LogicalFilter.create( join, rexBuilder.makeCall( SqlStdOperatorTable.IS_NULL, rexBuilder.makeInputRef( join, join.getRowType().getFieldCount() - 1) ) );
                final LogicalConditionalExecute lce = LogicalConditionalExecute.create( check, lceRoot, Condition.EQUAL_TO_ZERO, ConstraintViolationException.class,
                        String.format( "Insert violates foreign key constraint `%s`.`%s`", table.name, foreignKey.name ) );
                lce.setCheckDescription( String.format( "Enforcement of foreign key `%s`.`%s`", table.name, foreignKey.name ) );
                lceRoot = lce;
            }
        }

        //
        //  Enforce UNIQUE constraints in UPDATE operations
        //
        if ( root.isUpdate() || root.isMerge() ) {
            for ( final CatalogConstraint constraint : constraints ) {
                if ( constraint.type != ConstraintType.UNIQUE ) {
                    log.warn( "Unknown constraint type: " + constraint.type );
                    continue;
                }
                // Check if update affects this constraint
                for ( final String c : root.getUpdateColumnList() ) {
                    if ( constraint.key.getColumnNames().contains( c ) ) {
                        final Index index = IndexManager.getInstance().getIndex( schema, table, constraint.key.getColumnNames(), null, true );
                        // Delegate constraint enforcement to the index' duplicate check, only complain if no unique index is present
                        if ( index == null ) {
                            throw new IllegalStateException(
                                    String.format( "An unique index over `%s`.`%s` columns %s is required to provide enforcement of constraint `%s`.",
                                    schema.name, table.name, constraint.key.getColumnNames(), constraint.name ) );
                        }
                    }
                }
            }
        }

        //
        //  Enforce FOREIGN KEY constraints in UPDATE operations
        //
        if ( root.isUpdate() || root.isMerge() ) {
            RelBuilder builder = RelBuilder.create( statement );
            final RexBuilder rexBuilder = builder.getRexBuilder();
            for ( final CatalogForeignKey foreignKey : foreignKeys ) {
                final String constraintRule = "ON UPDATE " + foreignKey.updateRule;
                RelNode input = root.getInput();
                final List<RexNode> projects = new ArrayList<>( foreignKey.columnIds.size() );
                final List<RexNode> foreignProjects = new ArrayList<>( foreignKey.columnIds.size() );
                final CatalogTable foreignTable;
                try {
                    foreignTable = Catalog.getInstance().getTable( foreignKey.referencedKeyTableId );
                } catch ( UnknownTableException | GenericCatalogException e ) {
                    throw new RuntimeException( e );
                }
                builder.push( input );
                for ( int i = 0; i < foreignKey.columnIds.size(); ++i ) {
                    final String columnName = foreignKey.getColumnNames().get( i );
                    final String foreignColumnName = foreignKey.getReferencedKeyColumnNames().get( i );
                    final CatalogColumn foreignColumn;
                    try {
                        foreignColumn = Catalog.getInstance().getColumn( foreignTable.id, foreignColumnName );
                    } catch ( GenericCatalogException | UnknownColumnException e ) {
                        throw new RuntimeException( e );
                    }
                    RexNode newValue;
                    int targetIndex;
                    if ( root.isUpdate() ) {
                        targetIndex = root.getUpdateColumnList().indexOf( columnName );
                        newValue = root.getSourceExpressionList().get( targetIndex );
                        newValue = new RexShuttle() {
                            @Override
                            public RexNode visitFieldAccess( RexFieldAccess fieldAccess ) {
                                return rexBuilder.makeInputRef( input, input.getRowType().getField( fieldAccess.getField().getName(), true, false ).getIndex() );
                            }
                        }.apply( newValue );
                    } else {
                        targetIndex = input.getRowType().getField( columnName, true, false ).getIndex();
                        newValue = rexBuilder.makeInputRef( input, targetIndex );
                    }
                    RexNode foreignValue = rexBuilder.makeInputRef( foreignColumn.getRelDataType( rexBuilder.getTypeFactory() ) , targetIndex );
                    projects.add( newValue );
                    foreignProjects.add( foreignValue );
                }
                builder
                    .project( projects )
                        .scan( foreignKey.getReferencedKeyTableName() )
                    .project( foreignProjects );
                RexNode condition = rexBuilder.makeLiteral( true );
                for ( int i = 0; i < projects.size(); ++i) {
                    condition = builder.and(
                            condition,
                            builder.equals(
                                    builder.field( 2, 0, i ),
                                    builder.field( 2, 1, i )
                            )
                    );
                }
                final RelNode join = builder.join( JoinRelType.LEFT, condition ).build();
                final RelNode check = LogicalFilter.create( join, rexBuilder.makeCall( SqlStdOperatorTable.IS_NULL, rexBuilder.makeInputRef( join, projects.size() * 2 - 1 ) ) );
                final LogicalConditionalExecute lce = LogicalConditionalExecute.create( check, lceRoot, Condition.EQUAL_TO_ZERO, ConstraintViolationException.class,
                        String.format("Update violates foreign key constraint `%s` (`%s` %s -> `%s` %s, %s)",
                                foreignKey.name, table.name, foreignKey.getColumnNames(), foreignTable.name, foreignKey.getReferencedKeyColumnNames(), constraintRule ));
                lce.setCheckDescription( String.format( "Enforcement of foreign key `%s`.`%s`", table.name, foreignKey.name ) );
                lceRoot = lce;
            }
        }

        //
        //  Enforce reverse FOREIGN KEY constraints in UPDATE and DELETE operations
        //
        if ( root.isDelete() || root.isUpdate() || root.isMerge() ) {
            RelBuilder builder = RelBuilder.create( statement );
            final RexBuilder rexBuilder = builder.getRexBuilder();
            for ( final CatalogForeignKey foreignKey : exportedKeys ) {
                final String constraintRule = root.isDelete() ? "ON DELETE " + foreignKey.deleteRule : "ON UPDATE " + foreignKey.updateRule;
                switch ( root.isDelete() ? foreignKey.deleteRule : foreignKey.updateRule ) {
                    case RESTRICT:
                        break;
                    case CASCADE:
                    case SET_NULL:
                    case SET_DEFAULT:
                    default:
                        throw new NotImplementedException( String.format( "The foreign key option %s is not yet implemented.", constraintRule ) );
                }
                RelNode pInput = ((LogicalProject) root.getInput()).getInput();
                final List<RexNode> projects = new ArrayList<>( foreignKey.columnIds.size() );
                final List<RexNode> foreignProjects = new ArrayList<>( foreignKey.columnIds.size() );
                final CatalogTable foreignTable;
                try {
                    foreignTable = Catalog.getInstance().getTable( foreignKey.tableId );
                } catch ( UnknownTableException | GenericCatalogException e ) {
                    throw new RuntimeException( e );
                }
                for ( int i = 0; i < foreignKey.columnIds.size(); ++i ) {
                    final String columnName = foreignKey.getReferencedKeyColumnNames().get( i );
                    final String foreignColumnName = foreignKey.getColumnNames().get( i );
                    final CatalogColumn column, foreignColumn;
                    try {
                        column = Catalog.getInstance().getColumn( table.id, columnName );
                        foreignColumn = Catalog.getInstance().getColumn( foreignTable.id, foreignColumnName );
                    } catch ( GenericCatalogException | UnknownColumnException e ) {
                        throw new RuntimeException( e );
                    }
                    final RexNode inputRef = new RexInputRef( column.position - 1, rexBuilder.getTypeFactory().createPolyType( column.type ) );
                    final RexNode foreignInputRef = new RexInputRef( foreignColumn.position - 1, rexBuilder.getTypeFactory().createPolyType( foreignColumn.type ) );
                    projects.add( inputRef );
                    foreignProjects.add( foreignInputRef );
                }
                builder
                        .push( pInput )
                    .project( projects )
                        .scan( foreignKey.getTableName() )
                    .project( foreignProjects );
                RexNode condition = rexBuilder.makeLiteral( true );
                for ( int i = 0; i < projects.size(); ++i ) {
                    condition = builder.and(
                            condition,
                            builder.equals(
                                    builder.field( 2, 0, i ),
                                    builder.field( 2, 1, i )
                            )
                    );
                }
                final RelNode join = builder.join( JoinRelType.INNER, condition ).build();
                final LogicalConditionalExecute lce = LogicalConditionalExecute.create( join, lceRoot, Condition.EQUAL_TO_ZERO, ConstraintViolationException.class,
                        String.format("%s violates foreign key constraint `%s` (`%s` %s -> `%s` %s, %s)",
                                root.isUpdate() ? "Update" : "Delete",
                                foreignKey.name, foreignTable.name, foreignKey.getColumnNames(), table.name, foreignKey.getReferencedKeyColumnNames(), constraintRule ));
                lce.setCheckDescription( String.format( "Enforcement of foreign key `%s`.`%s`", foreignTable.name, foreignKey.name ) );
                lceRoot = lce;
            }
        }

        RelRoot enforcementRoot = new RelRoot( lceRoot, logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
        // Send the generated tree with all unoptimized constraint enforcement checks to the UI
        if ( statement.getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "Constraint Enforcement Plan" ).setLabel( "plans" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Constraint Enforcement Plan" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    group,
                    RelOptUtil.dumpPlan( "Constraint Enforcement Plan", enforcementRoot.rel, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        }
        return enforcementRoot;
    }


    private RelRoot indexLookup( RelRoot logicalRoot, Statement statement, ExecutionTimeMonitor executionTimeMonitor ) {
        final RelBuilder builder = RelBuilder.create( statement, logicalRoot.rel.getCluster() );
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
            final RelShuttle shuttle2 = new RelShuttleImpl() {

                @Override
                public RelNode visit( LogicalProject project ) {
                    if ( project.getInput() instanceof TableScan ) {
                        // Figure out the original column names required for index lookup
                        final TableScan scan = (TableScan) project.getInput();
                        final String table = scan.getTable().getQualifiedName().get( 0 );
                        final List<String> columns = new ArrayList<>( project.getChildExps().size() );
                        final List<RelDataType> ctypes = new ArrayList<>( project.getChildExps().size() );
                        for ( final RexNode expr : project.getChildExps()) {
                            if ( !( expr instanceof RexInputRef ) ) {
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
                        } catch ( UnknownTableException | GenericCatalogException e ) {
                            log.error( "Could not fetch table", e );
                            return super.visit( project );
                        }
                        // Retrieve any index and use for simplification
                        final Index idx = IndexManager.getInstance().getIndex( schema, ctable, columns );
                        if ( idx == null ) {
                            // No index available for simplification
                            return super.visit( project );
                        }
                        // TODO: Avoid copying stuff around
                        final RelDataType compositeType = builder.getTypeFactory().createStructType( ctypes, columns );
                        return idx.getAsValues( statement.getTransaction().getXid(), builder, compositeType );
//                        final ImmutableList<ImmutableList<RexLiteral>> tuples =
//                                ImmutableList.copyOf(idx.getAll().stream().map( ImmutableList::copyOf ).collect( Collectors.toList()));
//                        // TODO: Metadata regarding table name?
//                        // TODO: INSERT SELECT broken? Something, something, optimizer costs
//                        return LogicalValues.create( project.getCluster(), project.getRowType(), tuples );
                    }
                    return super.visit( project );
                }


                @Override
                public RelNode visit( RelNode node ) {
                    if ( node instanceof LogicalProject) {
                        final LogicalProject lp = (LogicalProject) node;
                        lp.getMapping();
                    }
                    return super.visit( node );
                }

            };
            final RelNode newRoot = shuttle2.visit( shuttle.visit( logicalRoot.rel ) );
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
