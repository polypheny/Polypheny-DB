/*
 * Copyright 2019-2023 The Polypheny Project
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


import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.avatica.Meta;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgVisitor;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Scan;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.CatalogGraphDatabase;
import org.polypheny.db.languages.NodeToAlgConverter;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.nodes.validate.ValidatorCatalogReader;
import org.polypheny.db.nodes.validate.ValidatorTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptEntity;
import org.polypheny.db.plan.AlgOptEntity.ToAlgContext;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexExecutorImpl;
import org.polypheny.db.runtime.Bindable;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.runtime.Typed;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.schema.Entity;
import org.polypheny.db.schema.ExtensibleEntity;
import org.polypheny.db.tools.Program;
import org.polypheny.db.tools.Programs;
import org.polypheny.db.util.Holder;
import org.polypheny.db.util.TryThreadLocal;
import org.polypheny.db.util.trace.PolyphenyDbTimingTracer;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
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
    protected AlgDataType parameterRowType;

    // temporary. for testing.
    public static final TryThreadLocal<Boolean> THREAD_TRIM = TryThreadLocal.of( false );

    /**
     * Temporary, until "Decorrelate sub-queries in Project and Join" is fixed.
     * <p>
     * The default is false, meaning do not expand queries during sql-to-rel, but a few tests override and set it to true.
     * After it is fixed, remove those overrides and use false everywhere.
     */
    public static final TryThreadLocal<Boolean> THREAD_EXPAND = TryThreadLocal.of( false );


    public Prepare( Context context, CatalogReader catalogReader, Convention resultConvention ) {
        assert context != null;
        this.context = context;
        this.catalogReader = catalogReader;
        this.resultConvention = resultConvention;
    }


    protected abstract PreparedResult createPreparedExplanation(
            AlgDataType resultType,
            AlgDataType parameterRowType,
            AlgRoot root,
            ExplainFormat format,
            ExplainLevel detailLevel );


    /**
     * Optimizes a query plan.
     *
     * @param root Root of relational expression tree
     * @return an equivalent optimized relational expression
     */
    protected AlgRoot optimize( AlgRoot root ) {
        final AlgOptPlanner planner = root.alg.getCluster().getPlanner();

        final DataContext dataContext = context.getDataContext();
        planner.setExecutor( new RexExecutorImpl( dataContext ) );

        final AlgTraitSet desiredTraits = getDesiredRootTraitSet( root );

        // Work around: Allow rules to be registered during planning process by briefly creating each kind of physical table
        // to let it register its rules.
        // The problem occurs when plans are created via AlgBuilder, not the usual process
        // (SQL and SqlToRelConverter.Config.isConvertTableAccess = true).
        final AlgVisitor visitor = new AlgVisitor() {
            @Override
            public void visit( AlgNode node, int ordinal, AlgNode parent ) {
                if ( node instanceof Scan ) {
                    final AlgOptCluster cluster = node.getCluster();
                    final ToAlgContext context = () -> cluster;
                    final AlgNode r = node.getEntity().toAlg( context, node.getTraitSet() );
                    planner.registerClass( r );
                }
                super.visit( node, ordinal, parent );
            }
        };
        visitor.go( root.alg );

        final Program program = getProgram();
        final AlgNode rootRel4 = program.run( planner, root.alg, desiredTraits );
        if ( LOGGER.isDebugEnabled() ) {
            LOGGER.debug( "Plan after physical tweaks: {}", AlgOptUtil.toString( rootRel4, ExplainLevel.ALL_ATTRIBUTES ) );
        }

        return root.withAlg( rootRel4 );
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


    protected AlgTraitSet getDesiredRootTraitSet( AlgRoot root ) {
        // Make sure non-CallingConvention traits, if any, are preserved
        return root.alg.getTraitSet()
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
    protected abstract PreparedResult implement( AlgRoot root );



    protected LogicalModify.Operation mapTableModOp( boolean isDml, Kind Kind ) {
        if ( !isDml ) {
            return null;
        }
        switch ( Kind ) {
            case INSERT:
                return LogicalModify.Operation.INSERT;
            case DELETE:
                return LogicalModify.Operation.DELETE;
            case MERGE:
                return LogicalModify.Operation.MERGE;
            case UPDATE:
                return LogicalModify.Operation.UPDATE;
            default:
                return null;
        }
    }


    /**
     * Protected method to allow subclasses to override construction of SqlToRelConverter.
     */
    //protected abstract NodeToAlgConverter getSqlToRelConverter( Validator validator, CatalogReader catalogReader, NodeToAlgConverter.Config config );
    public abstract AlgNode flattenTypes( AlgNode rootRel, boolean restructure );

    protected abstract AlgNode decorrelate( NodeToAlgConverter sqlToRelConverter, Node query, AlgNode rootRel );


    protected abstract void init( Class runtimeContextClass );

    protected abstract Validator getSqlValidator();


    /**
     * Interface by which validator and planner can read table metadata.
     */
    public interface CatalogReader extends AlgOptSchema, ValidatorCatalogReader, OperatorTable {

        @Override
        PreparingEntity getTableForMember( List<String> names );

        @Override
        PreparingEntity getTable( List<String> names );

        AlgOptEntity getCollection( List<String> names );

        CatalogGraphDatabase getGraph( String name );

        ThreadLocal<CatalogReader> THREAD_LOCAL = new ThreadLocal<>();

    }


    /**
     * Definition of a table, for the purposes of the validator and planner.
     */
    public interface PreparingEntity extends AlgOptEntity, ValidatorTable {

    }


    /**
     * Abstract implementation of {@link PreparingEntity}.
     */
    public abstract static class AbstractPreparingEntity implements PreparingEntity {

        @Override
        public final AlgOptEntity extend( List<AlgDataTypeField> extendedFields ) {
            final Entity entity = unwrap( Entity.class );

            // Get the set of extended columns that do not have the same name as a column in the base table.
            final List<AlgDataTypeField> baseColumns = getRowType().getFieldList();
            final List<AlgDataTypeField> dedupedFields = AlgOptUtil.deduplicateColumns( baseColumns, extendedFields );
            final List<AlgDataTypeField> dedupedExtendedFields = dedupedFields.subList( baseColumns.size(), dedupedFields.size() );

            if ( entity instanceof ExtensibleEntity ) {
                final Entity extendedEntity = ((ExtensibleEntity) entity).extend( dedupedExtendedFields );
                return extend( extendedEntity );
            }
            throw new RuntimeException( "Cannot extend " + entity );
        }


        /**
         * Implementation-specific code to instantiate a new {@link AlgOptEntity} based on a {@link Entity} that has been extended.
         */
        protected abstract AlgOptEntity extend( Entity extendedEntity );


        @Override
        public List<ColumnStrategy> getColumnStrategies() {
            return AlgOptEntityImpl.columnStrategies( AbstractPreparingEntity.this );
        }

    }


    /**
     * PreparedExplanation is a PreparedResult for an EXPLAIN PLAN statement. It's always good to have an explanation prepared.
     */
    public abstract static class PreparedExplain implements PreparedResult {

        private final AlgDataType rowType;
        private final AlgDataType parameterRowType;
        private final AlgRoot root;
        private final ExplainFormat format;
        private final ExplainLevel detailLevel;


        public PreparedExplain(
                AlgDataType rowType,
                AlgDataType parameterRowType,
                AlgRoot root,
                ExplainFormat format,
                ExplainLevel detailLevel ) {
            this.rowType = rowType;
            this.parameterRowType = parameterRowType;
            this.root = root;
            this.format = format;
            this.detailLevel = detailLevel;
        }


        @Override
        public String getCode() {
            if ( root == null ) {
                return AlgOptUtil.dumpType( rowType );
            } else {
                return AlgOptUtil.dumpPlan( "", root.alg, format, detailLevel );
            }
        }


        @Override
        public AlgDataType getParameterRowType() {
            return parameterRowType;
        }


        @Override
        public boolean isDml() {
            return false;
        }


        @Override
        public List<List<String>> getFieldOrigins() {
            return Collections.singletonList( Collections.nCopies( 4, null ) );
        }

    }


    /**
     * Result of a call to {}.
     */
    public interface PreparedResult {

        /**
         * Returns the code generated by preparation.
         */
        String getCode();

        /**
         * Returns whether this result is for a DML statement, in which case the result set is one row with one column
         * containing the number of rows affected.
         */
        boolean isDml();

        /**
         * Returns a list describing, for each result field, the origin of the field as a 4-element list
         * of (database, schema, table, column).
         */
        List<List<String>> getFieldOrigins();

        /**
         * Returns a record type whose fields are the parameters of this statement.
         */
        AlgDataType getParameterRowType();

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

        protected final AlgNode rootAlg;
        protected final AlgDataType parameterRowType;
        protected final AlgDataType rowType;
        protected final boolean isDml;
        protected final LogicalModify.Operation tableModOp;
        protected final List<List<String>> fieldOrigins;
        protected final List<AlgCollation> collations;


        public PreparedResultImpl(
                AlgDataType rowType,
                AlgDataType parameterRowType,
                List<List<String>> fieldOrigins,
                List<AlgCollation> collations,
                AlgNode rootAlg,
                LogicalModify.Operation tableModOp,
                boolean isDml ) {
            this.rowType = Objects.requireNonNull( rowType );
            this.parameterRowType = Objects.requireNonNull( parameterRowType );
            this.fieldOrigins = Objects.requireNonNull( fieldOrigins );
            this.collations = ImmutableList.copyOf( collations );
            this.rootAlg = Objects.requireNonNull( rootAlg );
            this.tableModOp = tableModOp;
            this.isDml = isDml;
        }


        @Override
        public boolean isDml() {
            return isDml;
        }


        @Override
        public List<List<String>> getFieldOrigins() {
            return fieldOrigins;
        }


        @Override
        public AlgDataType getParameterRowType() {
            return parameterRowType;
        }


        @Override
        public abstract Type getElementType();

    }

}

