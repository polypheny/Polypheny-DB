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

package org.polypheny.db.languages;

import org.polypheny.db.core.nodes.Node;
import org.polypheny.db.core.rel.RelFieldTrimmer;
import org.polypheny.db.core.rel.RelStructuredTypeFlattener;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rex.RexSubQuery;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.tools.RelBuilderFactory;

public interface NodeToRelConverter {

    /**
     * Size of the smallest IN list that will be converted to a semijoin to a static table.
     */
    int DEFAULT_IN_SUB_QUERY_THRESHOLD = 20;

    /**
     * Creates a builder for a {@link Config}.
     */
    static ConfigBuilder configBuilder() {
        return new ConfigBuilder();
    }

    void setDynamicParamCountInExplain( int explainParamCount );

    RelNode flattenTypes( RelNode rootRel, boolean restructure );

    RelNode decorrelate( Node query, RelNode rootRel );

    RelNode trimUnusedFields( boolean ordered, RelNode rootRel );

    RelFieldTrimmer newFieldTrimmer();

    RelRoot convertQuery( Node rawQuery, boolean needsValidation, boolean top );

    @Deprecated // to be removed before 2.0
    boolean isTrimUnusedFields();

    /**
     * Interface to define the configuration for a SqlToRelConverter.
     * Provides methods to set each configuration option.
     *
     * @see ConfigBuilder
     * @see NodeToRelConverter#configBuilder()
     */
    interface Config {

        /**
         * Default configuration.
         */
        Config DEFAULT = configBuilder().build();

        /**
         * Returns the {@code convertTableAccess} option. Controls whether table access references are converted to physical rels immediately. The optimizer doesn't like leaf rels to have {@link Convention#NONE}.
         * However, if we are doing further conversion passes (e.g. {@link RelStructuredTypeFlattener}), then we may need to defer conversion.
         */
        boolean isConvertTableAccess();

        /**
         * Returns the {@code decorrelationEnabled} option. Controls whether to disable sub-query decorrelation when needed. e.g. if outer joins are not supported.
         */
        boolean isDecorrelationEnabled();

        /**
         * Returns the {@code trimUnusedFields} option. Controls whether to trim unused fields as part of the conversion process.
         */
        boolean isTrimUnusedFields();

        /**
         * Returns the {@code createValuesRel} option. Controls whether instances of {@link LogicalValues} are generated.
         * These may not be supported by all physical implementations.
         */
        boolean isCreateValuesRel();

        /**
         * Returns the {@code explain} option. Describes whether the current statement is part of an EXPLAIN PLAN statement.
         */
        boolean isExplain();

        /**
         * Returns the {@code expand} option. Controls whether to expand sub-queries. If false, each sub-query becomes a {@link RexSubQuery}.
         */
        boolean isExpand();

        /**
         * Returns the {@code inSubQueryThreshold} option, default {@link #DEFAULT_IN_SUB_QUERY_THRESHOLD}. Controls the list size threshold under which {#@link #convertInToOr} is used. Lists of this size
         * or greater will instead be converted to use a join against an inline table ({@link LogicalValues}) rather than a predicate. A threshold of 0 forces usage of an inline table in all
         * cases; a threshold of {@link Integer#MAX_VALUE} forces usage of OR in all cases.
         */
        int getInSubQueryThreshold();

        /**
         * Returns the factory to create {@link RelBuilder}, never null. Default is {@link RelFactories#LOGICAL_BUILDER}.
         */
        RelBuilderFactory getRelBuilderFactory();

    }


    /**
     * Builder for a {@link Config}.
     */
    class ConfigBuilder {

        private boolean convertTableAccess = true;
        private boolean decorrelationEnabled = true;
        private boolean trimUnusedFields = false;
        private boolean createValuesRel = true;
        private boolean explain;
        private boolean expand = true;
        private int inSubQueryThreshold = DEFAULT_IN_SUB_QUERY_THRESHOLD;
        private RelBuilderFactory relBuilderFactory = RelFactories.LOGICAL_BUILDER;


        public ConfigBuilder() {
        }


        /**
         * Sets configuration identical to a given {@link Config}.
         */
        public ConfigBuilder withConfig( Config config ) {
            this.convertTableAccess = config.isConvertTableAccess();
            this.decorrelationEnabled = config.isDecorrelationEnabled();
            this.trimUnusedFields = config.isTrimUnusedFields();
            this.createValuesRel = config.isCreateValuesRel();
            this.explain = config.isExplain();
            this.expand = config.isExpand();
            this.inSubQueryThreshold = config.getInSubQueryThreshold();
            this.relBuilderFactory = config.getRelBuilderFactory();
            return this;
        }


        public ConfigBuilder withConvertTableAccess( boolean convertTableAccess ) {
            this.convertTableAccess = convertTableAccess;
            return this;
        }


        public ConfigBuilder withDecorrelationEnabled( boolean enabled ) {
            this.decorrelationEnabled = enabled;
            return this;
        }



        public ConfigBuilder withTrimUnusedFields( boolean trimUnusedFields ) {
            this.trimUnusedFields = trimUnusedFields;
            return this;
        }


        public ConfigBuilder withCreateValuesRel( boolean createValuesRel ) {
            this.createValuesRel = createValuesRel;
            return this;
        }


        public ConfigBuilder withExplain( boolean explain ) {
            this.explain = explain;
            return this;
        }


        public ConfigBuilder withExpand( boolean expand ) {
            this.expand = expand;
            return this;
        }


        public ConfigBuilder withInSubQueryThreshold( int inSubQueryThreshold ) {
            this.inSubQueryThreshold = inSubQueryThreshold;
            return this;
        }


        public ConfigBuilder withRelBuilderFactory( RelBuilderFactory relBuilderFactory ) {
            this.relBuilderFactory = relBuilderFactory;
            return this;
        }


        /**
         * Builds a {@link Config}.
         */
        public Config build() {
            return new ConfigImpl( convertTableAccess, decorrelationEnabled, trimUnusedFields, createValuesRel, explain, expand, inSubQueryThreshold, relBuilderFactory );
        }

    }


    /**
     * Implementation of {@link Config}.
     * Called by builder; all values are in private final fields.
     */
    class ConfigImpl implements Config {

        private final boolean convertTableAccess;
        private final boolean decorrelationEnabled;
        private final boolean trimUnusedFields;
        private final boolean createValuesRel;
        private final boolean explain;
        private final boolean expand;
        private final int inSubQueryThreshold;
        private final RelBuilderFactory relBuilderFactory;


        private ConfigImpl( boolean convertTableAccess, boolean decorrelationEnabled, boolean trimUnusedFields, boolean createValuesRel, boolean explain, boolean expand, int inSubQueryThreshold, RelBuilderFactory relBuilderFactory ) {
            this.convertTableAccess = convertTableAccess;
            this.decorrelationEnabled = decorrelationEnabled;
            this.trimUnusedFields = trimUnusedFields;
            this.createValuesRel = createValuesRel;
            this.explain = explain;
            this.expand = expand;
            this.inSubQueryThreshold = inSubQueryThreshold;
            this.relBuilderFactory = relBuilderFactory;
        }


        @Override
        public boolean isConvertTableAccess() {
            return convertTableAccess;
        }


        @Override
        public boolean isDecorrelationEnabled() {
            return decorrelationEnabled;
        }


        @Override
        public boolean isTrimUnusedFields() {
            return trimUnusedFields;
        }


        @Override
        public boolean isCreateValuesRel() {
            return createValuesRel;
        }


        @Override
        public boolean isExplain() {
            return explain;
        }


        @Override
        public boolean isExpand() {
            return expand;
        }


        @Override
        public int getInSubQueryThreshold() {
            return inSubQueryThreshold;
        }


        @Override
        public RelBuilderFactory getRelBuilderFactory() {
            return relBuilderFactory;
        }

    }

}
