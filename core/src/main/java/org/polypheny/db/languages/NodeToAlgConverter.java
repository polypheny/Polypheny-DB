/*
 * Copyright 2019-2024 The Polypheny Project
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

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.polypheny.db.algebra.AlgFieldTrimmer;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgStructuredTypeFlattener;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexSubQuery;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilderFactory;

public interface NodeToAlgConverter {

    /**
     * Size of the smallest IN list that will be converted to a semi-join to a static table.
     */
    int DEFAULT_IN_SUB_QUERY_THRESHOLD = 20;

    /**
     * Creates a builder for a {@link Config}.
     */
    static ConfigBuilder configBuilder() {
        return new ConfigBuilder();
    }

    AlgNode flattenTypes( AlgNode rootAlg, boolean restructure );

    AlgNode decorrelate( Node query, AlgNode rootAlg );

    AlgNode trimUnusedFields( boolean ordered, AlgNode rootAlg );

    AlgFieldTrimmer newFieldTrimmer();

    AlgRoot convertQuery( Node rawQuery, boolean needsValidation, boolean top );

    @Deprecated
        // to be removed before 2.0
    boolean isTrimUnusedFields();

    /**
     * Interface to define the configuration for a NodeToRelConverter.
     * Provides methods to set each configuration option.
     *
     * @see NodeToAlgConverter#configBuilder()
     */
    interface Config {

        /**
         * Default configuration.
         */
        Config DEFAULT = configBuilder().build();

        /**
         * Returns the {@code convertTableAccess} option. Controls whether table access references are converted to physical algs immediately. The optimizer doesn't like leaf algs to have {@link Convention#NONE}.
         * However, if we are doing further conversion passes (e.g. {@link AlgStructuredTypeFlattener}), then we may need to defer conversion.
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
         * Returns the {@code createValuesRel} option. Controls whether instances of {@link LogicalRelValues} are generated.
         * These may not be supported by all physical implementations.
         */
        boolean isCreateValuesAlg();

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
         * or greater will instead be converted to use a join against an inline table ({@link LogicalRelValues}) rather than a predicate. A threshold of 0 forces usage of an inline table in all
         * cases; a threshold of {@link Integer#MAX_VALUE} forces usage of OR in all cases.
         */
        int getInSubQueryThreshold();

        /**
         * Returns the factory to create {@link AlgBuilder}, never null. Default is {@link AlgFactories#LOGICAL_BUILDER}.
         */
        AlgBuilderFactory getAlgBuilderFactory();

    }


    /**
     * Builder for a {@link Config}.
     */
    @Setter
    @Accessors(fluent = true)
    @NoArgsConstructor
    class ConfigBuilder {

        private boolean convertTableAccess = true;
        private boolean decorrelationEnabled = true;
        private boolean trimUnusedFields = false;
        private boolean createValuesAlg = true;
        private boolean explain;
        private boolean expand = true;
        private int inSubQueryThreshold = DEFAULT_IN_SUB_QUERY_THRESHOLD;
        private AlgBuilderFactory algBuilderFactory = AlgFactories.LOGICAL_BUILDER;


        /**
         * Sets configuration identical to a given {@link Config}.
         */
        public ConfigBuilder config( Config config ) {
            this.convertTableAccess = config.isConvertTableAccess();
            this.decorrelationEnabled = config.isDecorrelationEnabled();
            this.trimUnusedFields = config.isTrimUnusedFields();
            this.createValuesAlg = config.isCreateValuesAlg();
            this.explain = config.isExplain();
            this.expand = config.isExpand();
            this.inSubQueryThreshold = config.getInSubQueryThreshold();
            this.algBuilderFactory = config.getAlgBuilderFactory();
            return this;
        }


        /**
         * Builds a {@link Config}.
         */
        public Config build() {
            return new ConfigImpl( convertTableAccess, decorrelationEnabled, trimUnusedFields, createValuesAlg, explain, expand, inSubQueryThreshold, algBuilderFactory );
        }

    }


    /**
     * Implementation of {@link Config}.
     * Called by builder; all values are in private final fields.
     */
    @Getter
    @AllArgsConstructor
    class ConfigImpl implements Config {

        private final boolean convertTableAccess;
        private final boolean decorrelationEnabled;
        private final boolean trimUnusedFields;
        private final boolean createValuesAlg;
        private final boolean explain;
        private final boolean expand;
        private final int inSubQueryThreshold;
        private final AlgBuilderFactory algBuilderFactory;

    }

}
