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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.prepare;


import org.polypheny.db.DataContext;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptSchema;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.plan.ViewExpanders;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.RelVisitor;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexExecutorImpl;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.runtime.Typed;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.schema.ExtensibleTable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.impl.ModifiableViewTable;
import org.polypheny.db.sql.SqlExplain;
import org.polypheny.db.sql.SqlExplainFormat;
import org.polypheny.db.sql.SqlExplainLevel;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperatorTable;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.sql.validate.SqlValidatorCatalogReader;
import org.polypheny.db.sql.validate.SqlValidatorTable;
import org.polypheny.db.sql2rel.SqlToRelConverter;
import org.polypheny.db.tools.Program;
import org.polypheny.db.tools.Programs;
import org.polypheny.db.util.Holder;
import org.polypheny.db.util.TryThreadLocal;
import org.polypheny.db.util.trace.PolyphenyDbTimingTracer;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
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
     * Temporary, until "Decorrelate sub-queries in Project and Join" is fixed.
     *
     * The default is false, meaning do not expand queries during sql-to-rel, but a few tests override and set it to true. After it is fixed, remove those overrides and use false everywhere.
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
     * @return an equivalent optimized relational expression
     */
    protected RelRoot optimize( RelRoot root ) {
        final RelOptPlanner planner = root.rel.getCluster().getPlanner();

        final DataContext dataContext = context.getDataContext();
        planner.setExecutor( new RexExecutorImpl( dataContext ) );

        final RelTraitSet desiredTraits = getDesiredRootTraitSet( root );

        // Work around: Allow rules to be registered during planning process by briefly creating each kind of physical table to let it register its rules.
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
        final RelNode rootRel4 = program.run( planner, root.rel, desiredTraits );
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
                    root = optimize( root );
                    return createPreparedExplanation( null, parameterRowType, root, sqlExplain.getFormat(), sqlExplain.getDetailLevel() );
            }
        }

        root = optimize( root );

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

        @Override
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
     * Abstract implementation of {@link PreparingTable}.
     */
    public abstract static class AbstractPreparingTable implements PreparingTable {

        @Override
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


        @Override
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


        @Override
        public String getCode() {
            if ( root == null ) {
                return RelOptUtil.dumpType( rowType );
            } else {
                return RelOptUtil.dumpPlan( "", root.rel, format, detailLevel );
            }
        }


        @Override
        public RelDataType getParameterRowType() {
            return parameterRowType;
        }


        @Override
        public boolean isDml() {
            return false;
        }


        @Override
        public LogicalTableModify.Operation getTableModOp() {
            return null;
        }


        @Override
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


        public PreparedResultImpl(
                RelDataType rowType,
                RelDataType parameterRowType,
                List<List<String>> fieldOrigins,
                List<RelCollation> collations,
                RelNode rootRel,
                LogicalTableModify.Operation tableModOp,
                boolean isDml ) {
            this.rowType = Objects.requireNonNull( rowType );
            this.parameterRowType = Objects.requireNonNull( parameterRowType );
            this.fieldOrigins = Objects.requireNonNull( fieldOrigins );
            this.collations = ImmutableList.copyOf( collations );
            this.rootRel = Objects.requireNonNull( rootRel );
            this.tableModOp = tableModOp;
            this.isDml = isDml;
        }


        @Override
        public boolean isDml() {
            return isDml;
        }


        @Override
        public LogicalTableModify.Operation getTableModOp() {
            return tableModOp;
        }


        @Override
        public List<List<String>> getFieldOrigins() {
            return fieldOrigins;
        }


        @Override
        public RelDataType getParameterRowType() {
            return parameterRowType;
        }


        /**
         * Returns the physical row type of this prepared statement. May not be identical to the row type returned by the validator; for example, the field names may have been made unique.
         */
        public RelDataType getPhysicalRowType() {
            return rowType;
        }


        @Override
        public abstract Type getElementType();


        public RelNode getRootRel() {
            return rootRel;
        }
    }

}

