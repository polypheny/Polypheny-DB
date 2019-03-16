/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.prepare;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.Context;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbSchema.LatticeEntry;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbSchema.TableEntry;
import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptLattice;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptMaterialization;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptSchema;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.plan.ViewExpanders;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelRoot;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelVisitor;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexExecutorImpl;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Bindable;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Typed;
import ch.unibas.dmi.dbis.polyphenydb.schema.ColumnStrategy;
import ch.unibas.dmi.dbis.polyphenydb.schema.ExtensibleTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.Wrapper;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.ModifiableViewTable;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.StarTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplain;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainFormat;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorCatalogReader;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.InitializerContext;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.InitializerExpressionFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.SqlToRelConverter;
import ch.unibas.dmi.dbis.polyphenydb.tools.Program;
import ch.unibas.dmi.dbis.polyphenydb.tools.Programs;
import ch.unibas.dmi.dbis.polyphenydb.util.Holder;
import ch.unibas.dmi.dbis.polyphenydb.util.TryThreadLocal;
import ch.unibas.dmi.dbis.polyphenydb.util.trace.PolyphenyDbTimingTracer;
import ch.unibas.dmi.dbis.polyphenydb.util.trace.PolyphenyDbTrace;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.avatica.Meta;
import org.slf4j.Logger;


/**
 * Abstract base for classes that implement the process of preparing and executing SQL expressions.
 */
public abstract class Prepare {

    protected static final Logger LOGGER = PolyphenyDbTrace.getStatementTracer();

    protected final Context context;
    protected final CatalogReader catalogReader;
    /**
     * Convention via which results should be returned by execution.
     */
    protected final Convention resultConvention;
    protected PolyphenyDbTimingTracer timingTracer;
    protected List<List<String>> fieldOrigins;
    protected RelDataType parameterRowType;

    // temporary. for testing.
    public static final TryThreadLocal<Boolean> THREAD_TRIM = TryThreadLocal.of( false );

    /**
     * Temporary, until <a href="https://issues.apache.org/jira/browse/CALCITE-1045">[POLYPHENYDB-1045] Decorrelate sub-queries in Project and Join</a> is fixed.
     *
     * The default is false, meaning do not expand queries during sql-to-rel, but a few tests override and set it to true. After POLYPHENYDB-1045 is fixed, remove those overrides and use false everywhere.
     */
    public static final TryThreadLocal<Boolean> THREAD_EXPAND = TryThreadLocal.of( false );


    public Prepare( Context context, CatalogReader catalogReader, Convention resultConvention ) {
        assert context != null;
        this.context = context;
        this.catalogReader = catalogReader;
        this.resultConvention = resultConvention;
    }


    protected abstract PreparedResult createPreparedExplanation(
            RelDataType resultType,
            RelDataType parameterRowType,
            RelRoot root,
            SqlExplainFormat format,
            SqlExplainLevel detailLevel );


    /**
     * Optimizes a query plan.
     *
     * @param root Root of relational expression tree
     * @param materializations Tables known to be populated with a given query
     * @param lattices Lattices
     * @return an equivalent optimized relational expression
     */
    protected RelRoot optimize( RelRoot root, final List<Materialization> materializations, final List<LatticeEntry> lattices ) {
        final RelOptPlanner planner = root.rel.getCluster().getPlanner();

        final DataContext dataContext = context.getDataContext();
        planner.setExecutor( new RexExecutorImpl( dataContext ) );

        final List<RelOptMaterialization> materializationList = new ArrayList<>();
        for ( Materialization materialization : materializations ) {
            List<String> qualifiedTableName = materialization.materializedTable.path();
            materializationList.add(
                    new RelOptMaterialization(
                            materialization.tableRel,
                            materialization.queryRel,
                            materialization.starRelOptTable,
                            qualifiedTableName ) );
        }

        final List<RelOptLattice> latticeList = new ArrayList<>();
        for ( LatticeEntry lattice : lattices ) {
            final TableEntry starTable = lattice.getStarTable();
            final JavaTypeFactory typeFactory = context.getTypeFactory();
            final RelOptTableImpl starRelOptTable =
                    RelOptTableImpl.create(
                            catalogReader,
                            starTable.getTable().getRowType( typeFactory ),
                            starTable,
                            null );
            latticeList.add( new RelOptLattice( lattice.getLattice(), starRelOptTable ) );
        }

        final RelTraitSet desiredTraits = getDesiredRootTraitSet( root );

        // Work around [POLYPHENYDB-1774] Allow rules to be registered during planning process by briefly creating each kind of physical table to let it register its rules.
        // The problem occurs when plans are created via RelBuilder, not the usual process (SQL and SqlToRelConverter.Config.isConvertTableAccess = true).
        final RelVisitor visitor = new RelVisitor() {
            @Override
            public void visit( RelNode node, int ordinal, RelNode parent ) {
                if ( node instanceof TableScan ) {
                    final RelOptCluster cluster = node.getCluster();
                    final RelOptTable.ToRelContext context = ViewExpanders.simpleContext( cluster );
                    final RelNode r = node.getTable().toRel( context );
                    planner.registerClass( r );
                }
                super.visit( node, ordinal, parent );
            }
        };
        visitor.go( root.rel );

        final Program program = getProgram();
        final RelNode rootRel4 = program.run( planner, root.rel, desiredTraits, materializationList, latticeList );
        if ( LOGGER.isDebugEnabled() ) {
            LOGGER.debug( "Plan after physical tweaks: {}", RelOptUtil.toString( rootRel4, SqlExplainLevel.ALL_ATTRIBUTES ) );
        }

        return root.withRel( rootRel4 );
    }


    protected Program getProgram() {
        // Allow a test to override the default program.
        final Holder<Program> holder = Holder.of( null );
        Hook.PROGRAM.run( holder );
        if ( holder.get() != null ) {
            return holder.get();
        }

        return Programs.standard();
    }


    protected RelTraitSet getDesiredRootTraitSet( RelRoot root ) {
        // Make sure non-CallingConvention traits, if any, are preserved
        return root.rel.getTraitSet()
                .replace( resultConvention )
                .replace( root.collation )
                .simplify();
    }


    /**
     * Implements a physical query plan.
     *
     * @param root Root of the relational expression tree
     * @return an executable plan
     */
    protected abstract PreparedResult implement( RelRoot root );


    public PreparedResult prepareSql( SqlNode sqlQuery, Class runtimeContextClass, SqlValidator validator, boolean needsValidation ) {
        return prepareSql( sqlQuery, sqlQuery, runtimeContextClass, validator, needsValidation );
    }


    public PreparedResult prepareSql( SqlNode sqlQuery, SqlNode sqlNodeOriginal, Class runtimeContextClass, SqlValidator validator, boolean needsValidation ) {
        init( runtimeContextClass );

        final SqlToRelConverter.ConfigBuilder builder =
                SqlToRelConverter.configBuilder()
                        .withTrimUnusedFields( true )
                        .withExpand( THREAD_EXPAND.get() )
                        .withExplain( sqlQuery.getKind() == SqlKind.EXPLAIN );
        final SqlToRelConverter sqlToRelConverter = getSqlToRelConverter( validator, catalogReader, builder.build() );

        SqlExplain sqlExplain = null;
        if ( sqlQuery.getKind() == SqlKind.EXPLAIN ) {
            // dig out the underlying SQL statement
            sqlExplain = (SqlExplain) sqlQuery;
            sqlQuery = sqlExplain.getExplicandum();
            sqlToRelConverter.setDynamicParamCountInExplain( sqlExplain.getDynamicParamCount() );
        }

        RelRoot root = sqlToRelConverter.convertQuery( sqlQuery, needsValidation, true );
        Hook.CONVERTED.run( root.rel );

        if ( timingTracer != null ) {
            timingTracer.traceTime( "end sql2rel" );
        }

        final RelDataType resultType = validator.getValidatedNodeType( sqlQuery );
        fieldOrigins = validator.getFieldOrigins( sqlQuery );
        assert fieldOrigins.size() == resultType.getFieldCount();

        parameterRowType = validator.getParameterRowType( sqlQuery );

        // Display logical plans before view expansion, plugging in physical storage and decorrelation
        if ( sqlExplain != null ) {
            SqlExplain.Depth explainDepth = sqlExplain.getDepth();
            SqlExplainFormat format = sqlExplain.getFormat();
            SqlExplainLevel detailLevel = sqlExplain.getDetailLevel();
            switch ( explainDepth ) {
                case TYPE:
                    return createPreparedExplanation( resultType, parameterRowType, null, format, detailLevel );
                case LOGICAL:
                    return createPreparedExplanation( null, parameterRowType, root, format, detailLevel );
                default:
            }
        }

        // Structured type flattening, view expansion, and plugging in physical storage.
        root = root.withRel( flattenTypes( root.rel, true ) );

        if ( this.context.config().forceDecorrelate() ) {
            // Sub-query decorrelation.
            root = root.withRel( decorrelate( sqlToRelConverter, sqlQuery, root.rel ) );
        }

        // Trim unused fields.
        root = trimUnusedFields( root );

        Hook.TRIMMED.run( root.rel );

        // Display physical plan after decorrelation.
        if ( sqlExplain != null ) {
            switch ( sqlExplain.getDepth() ) {
                case PHYSICAL:
                default:
                    root = optimize( root, getMaterializations(), getLattices() );
                    return createPreparedExplanation( null, parameterRowType, root, sqlExplain.getFormat(), sqlExplain.getDetailLevel() );
            }
        }

        root = optimize( root, getMaterializations(), getLattices() );

        if ( timingTracer != null ) {
            timingTracer.traceTime( "end optimization" );
        }

        // For transformation from DML -> DML, use result of rewrite (e.g. UPDATE -> MERGE).  For anything else (e.g. CALL -> SELECT), use original kind.
        if ( !root.kind.belongsTo( SqlKind.DML ) ) {
            root = root.withKind( sqlNodeOriginal.getKind() );
        }
        return implement( root );
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


    /**
     * Protected method to allow subclasses to override construction of SqlToRelConverter.
     */
    protected abstract SqlToRelConverter getSqlToRelConverter( SqlValidator validator, CatalogReader catalogReader, SqlToRelConverter.Config config );

    public abstract RelNode flattenTypes( RelNode rootRel, boolean restructure );

    protected abstract RelNode decorrelate( SqlToRelConverter sqlToRelConverter, SqlNode query, RelNode rootRel );

    protected abstract List<Materialization> getMaterializations();

    protected abstract List<LatticeEntry> getLattices();


    /**
     * Walks over a tree of relational expressions, replacing each {@link RelNode} with a 'slimmed down' relational expression that projects only the columns required by its consumer.
     *
     * @param root Root of relational expression tree
     * @return Trimmed relational expression
     */
    protected RelRoot trimUnusedFields( RelRoot root ) {
        final SqlToRelConverter.Config config = SqlToRelConverter.configBuilder()
                .withTrimUnusedFields( shouldTrim( root.rel ) )
                .withExpand( THREAD_EXPAND.get() )
                .build();
        final SqlToRelConverter converter = getSqlToRelConverter( getSqlValidator(), catalogReader, config );
        final boolean ordered = !root.collation.getFieldCollations().isEmpty();
        final boolean dml = SqlKind.DML.contains( root.kind );
        return root.withRel( converter.trimUnusedFields( dml || ordered, root.rel ) );
    }


    private boolean shouldTrim( RelNode rootRel ) {
        // For now, don't trim if there are more than 3 joins. The projects near the leaves created by trim migrate past joins and seem to prevent join-reordering.
        return THREAD_TRIM.get() || RelOptUtil.countJoins( rootRel ) < 2;
    }


    protected abstract void init( Class runtimeContextClass );

    protected abstract SqlValidator getSqlValidator();


    /**
     * Interface by which validator and planner can read table metadata.
     */
    public interface CatalogReader extends RelOptSchema, SqlValidatorCatalogReader, SqlOperatorTable {

        PreparingTable getTableForMember( List<String> names );

        /**
         * Returns a catalog reader the same as this one but with a possibly different schema path.
         */
        CatalogReader withSchemaPath( List<String> schemaPath );

        @Override
        PreparingTable getTable( List<String> names );

        ThreadLocal<CatalogReader> THREAD_LOCAL = new ThreadLocal<>();
    }


    /**
     * Definition of a table, for the purposes of the validator and planner.
     */
    public interface PreparingTable extends RelOptTable, SqlValidatorTable {

    }


    /**
     * Abstract implementation of {@link PreparingTable} with an implementation for {@link #columnHasDefaultValue}.
     */
    public abstract static class AbstractPreparingTable implements PreparingTable {

        @SuppressWarnings("deprecation")
        public boolean columnHasDefaultValue( RelDataType rowType, int ordinal, InitializerContext initializerContext ) {
            // This method is no longer used
            final Table table = this.unwrap( Table.class );
            if ( table != null && table instanceof Wrapper ) {
                final InitializerExpressionFactory initializerExpressionFactory = ((Wrapper) table).unwrap( InitializerExpressionFactory.class );
                if ( initializerExpressionFactory != null ) {
                    return initializerExpressionFactory
                            .newColumnDefaultValue( this, ordinal, initializerContext )
                            .getType()
                            .getSqlTypeName() != SqlTypeName.NULL;
                }
            }
            if ( ordinal >= rowType.getFieldList().size() ) {
                return true;
            }
            return !rowType.getFieldList().get( ordinal ).getType().isNullable();
        }


        public final RelOptTable extend( List<RelDataTypeField> extendedFields ) {
            final Table table = unwrap( Table.class );

            // Get the set of extended columns that do not have the same name as a column in the base table.
            final List<RelDataTypeField> baseColumns = getRowType().getFieldList();
            final List<RelDataTypeField> dedupedFields = RelOptUtil.deduplicateColumns( baseColumns, extendedFields );
            final List<RelDataTypeField> dedupedExtendedFields = dedupedFields.subList( baseColumns.size(), dedupedFields.size() );

            if ( table instanceof ExtensibleTable ) {
                final Table extendedTable = ((ExtensibleTable) table).extend( dedupedExtendedFields );
                return extend( extendedTable );
            } else if ( table instanceof ModifiableViewTable ) {
                final ModifiableViewTable modifiableViewTable = (ModifiableViewTable) table;
                final ModifiableViewTable extendedView = modifiableViewTable.extend( dedupedExtendedFields, getRelOptSchema().getTypeFactory() );
                return extend( extendedView );
            }
            throw new RuntimeException( "Cannot extend " + table );
        }


        /**
         * Implementation-specific code to instantiate a new {@link RelOptTable} based on a {@link Table} that has been extended.
         */
        protected abstract RelOptTable extend( Table extendedTable );


        public List<ColumnStrategy> getColumnStrategies() {
            return RelOptTableImpl.columnStrategies( AbstractPreparingTable.this );
        }
    }


    /**
     * PreparedExplanation is a PreparedResult for an EXPLAIN PLAN statement. It's always good to have an explanation prepared.
     */
    public abstract static class PreparedExplain implements PreparedResult {

        private final RelDataType rowType;
        private final RelDataType parameterRowType;
        private final RelRoot root;
        private final SqlExplainFormat format;
        private final SqlExplainLevel detailLevel;


        public PreparedExplain( RelDataType rowType, RelDataType parameterRowType, RelRoot root, SqlExplainFormat format, SqlExplainLevel detailLevel ) {
            this.rowType = rowType;
            this.parameterRowType = parameterRowType;
            this.root = root;
            this.format = format;
            this.detailLevel = detailLevel;
        }


        public String getCode() {
            if ( root == null ) {
                return RelOptUtil.dumpType( rowType );
            } else {
                return RelOptUtil.dumpPlan( "", root.rel, format, detailLevel );
            }
        }


        public RelDataType getParameterRowType() {
            return parameterRowType;
        }


        public boolean isDml() {
            return false;
        }


        public LogicalTableModify.Operation getTableModOp() {
            return null;
        }


        public List<List<String>> getFieldOrigins() {
            return Collections.singletonList( Collections.nCopies( 4, null ) );
        }
    }


    /**
     * Result of a call to {@link Prepare#prepareSql}.
     */
    public interface PreparedResult {

        /**
         * Returns the code generated by preparation.
         */
        String getCode();

        /**
         * Returns whether this result is for a DML statement, in which case the result set is one row with one column containing the number of rows affected.
         */
        boolean isDml();

        /**
         * Returns the table modification operation corresponding to this statement if it is a table modification statement; otherwise null.
         */
        LogicalTableModify.Operation getTableModOp();

        /**
         * Returns a list describing, for each result field, the origin of the field as a 4-element list of (database, schema, table, column).
         */
        List<List<String>> getFieldOrigins();

        /**
         * Returns a record type whose fields are the parameters of this statement.
         */
        RelDataType getParameterRowType();

        /**
         * Executes the prepared result.
         *
         * @param cursorFactory How to map values into a cursor
         * @return producer of rows resulting from execution
         */
        Bindable getBindable( Meta.CursorFactory cursorFactory );
    }


    /**
     * Abstract implementation of {@link PreparedResult}.
     */
    public abstract static class PreparedResultImpl implements PreparedResult, Typed {

        protected final RelNode rootRel;
        protected final RelDataType parameterRowType;
        protected final RelDataType rowType;
        protected final boolean isDml;
        protected final LogicalTableModify.Operation tableModOp;
        protected final List<List<String>> fieldOrigins;
        protected final List<RelCollation> collations;


        public PreparedResultImpl( RelDataType rowType, RelDataType parameterRowType, List<List<String>> fieldOrigins, List<RelCollation> collations, RelNode rootRel, LogicalTableModify.Operation tableModOp, boolean isDml ) {
            this.rowType = Objects.requireNonNull( rowType );
            this.parameterRowType = Objects.requireNonNull( parameterRowType );
            this.fieldOrigins = Objects.requireNonNull( fieldOrigins );
            this.collations = ImmutableList.copyOf( collations );
            this.rootRel = Objects.requireNonNull( rootRel );
            this.tableModOp = tableModOp;
            this.isDml = isDml;
        }


        public boolean isDml() {
            return isDml;
        }


        public LogicalTableModify.Operation getTableModOp() {
            return tableModOp;
        }


        public List<List<String>> getFieldOrigins() {
            return fieldOrigins;
        }


        public RelDataType getParameterRowType() {
            return parameterRowType;
        }


        /**
         * Returns the physical row type of this prepared statement. May not be identical to the row type returned by the validator; for example, the field names may have been made unique.
         */
        public RelDataType getPhysicalRowType() {
            return rowType;
        }


        public abstract Type getElementType();


        public RelNode getRootRel() {
            return rootRel;
        }
    }


    /**
     * Describes that a given SQL query is materialized by a given table. The materialization is currently valid, and can be used in the planning process.
     */
    public static class Materialization {

        /**
         * The table that holds the materialized data.
         */
        final TableEntry materializedTable;
        /**
         * The query that derives the data.
         */
        final String sql;
        /**
         * The schema path for the query.
         */
        final List<String> viewSchemaPath;
        /**
         * Relational expression for the table. Usually a {@link LogicalTableScan}.
         */
        RelNode tableRel;
        /**
         * Relational expression for the query to populate the table.
         */
        RelNode queryRel;
        /**
         * Star table identified.
         */
        private RelOptTable starRelOptTable;


        public Materialization( TableEntry materializedTable, String sql, List<String> viewSchemaPath ) {
            assert materializedTable != null;
            assert sql != null;
            this.materializedTable = materializedTable;
            this.sql = sql;
            this.viewSchemaPath = viewSchemaPath;
        }


        public void materialize( RelNode queryRel, RelOptTable starRelOptTable ) {
            this.queryRel = queryRel;
            this.starRelOptTable = starRelOptTable;
            assert starRelOptTable.unwrap( StarTable.class ) != null;
        }
    }
}

