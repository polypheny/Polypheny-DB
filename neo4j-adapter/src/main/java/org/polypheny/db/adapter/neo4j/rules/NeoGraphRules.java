/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adapter.neo4j.rules;

import java.util.function.Predicate;
import org.polypheny.db.adapter.neo4j.NeoConvention;
import org.polypheny.db.adapter.neo4j.NeoToEnumerableConverterRule;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoGraphAggregate;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoGraphFilter;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoGraphMatch;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoGraphModify;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoGraphProject;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoGraphSort;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoGraphUnwind;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoGraphValues;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.logical.graph.GraphAggregate;
import org.polypheny.db.algebra.logical.graph.GraphFilter;
import org.polypheny.db.algebra.logical.graph.GraphMatch;
import org.polypheny.db.algebra.logical.graph.GraphModify;
import org.polypheny.db.algebra.logical.graph.GraphProject;
import org.polypheny.db.algebra.logical.graph.GraphSort;
import org.polypheny.db.algebra.logical.graph.GraphUnwind;
import org.polypheny.db.algebra.logical.graph.GraphValues;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.Convention;

public interface NeoGraphRules {

    AlgOptRule[] RULES = {
            NeoToEnumerableConverterRule.INSTANCE,
            NeoGraphModifyRule.INSTANCE,
            NeoGraphProjectRule.INSTANCE,
            NeoGraphFilterRule.INSTANCE,
            NeoGraphValuesRule.INSTANCE,
            NeoGraphSortRule.INSTANCE,
            NeoGraphUnwindRule.INSTANCE,
            NeoGraphAggregateRule.INSTANCE,
            NeoGraphMatchRule.INSTANCE
    };


    abstract class NeoConverterRule extends ConverterRule {

        protected final Convention out;


        public <R extends AlgNode> NeoConverterRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, Convention.NONE, NeoConvention.INSTANCE, AlgFactories.LOGICAL_BUILDER, description );
            this.out = NeoConvention.INSTANCE;
        }

    }


    class NeoGraphModifyRule extends NeoConverterRule {

        public static NeoGraphModifyRule INSTANCE = new NeoGraphModifyRule( GraphModify.class, r -> true, "NeoGraphModifyRule" );


        private <R extends AlgNode> NeoGraphModifyRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            GraphModify modify = (GraphModify) alg;
            return new NeoGraphModify(
                    modify.getCluster(),
                    modify.getTraitSet().replace( NeoConvention.INSTANCE ),
                    modify.getGraph(),
                    convert( modify.getInput(), NeoConvention.INSTANCE ),
                    modify.operation,
                    modify.ids,
                    modify.operations );
        }

    }


    class NeoGraphProjectRule extends NeoConverterRule {

        public static NeoGraphProjectRule INSTANCE = new NeoGraphProjectRule( GraphProject.class, r -> true, "NeoGraphProjectRule" );


        private <R extends AlgNode> NeoGraphProjectRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            GraphProject project = (GraphProject) alg;
            return new NeoGraphProject(
                    alg.getCluster(),
                    alg.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( project.getInput(), NeoConvention.INSTANCE ),
                    project.getProjects() );
        }

    }


    class NeoGraphFilterRule extends NeoConverterRule {

        public static NeoGraphFilterRule INSTANCE = new NeoGraphFilterRule( GraphFilter.class, r -> true, "NeoGraphFilterRule" );


        private <R extends AlgNode> NeoGraphFilterRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public GraphFilter convert( AlgNode alg ) {
            GraphFilter filter = (GraphFilter) alg;
            return new NeoGraphFilter(
                    filter.getCluster(),
                    filter.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( filter.getInput(), NeoConvention.INSTANCE ),
                    filter.getCondition() );
        }

    }


    class NeoGraphValuesRule extends NeoConverterRule {

        public static NeoGraphValuesRule INSTANCE = new NeoGraphValuesRule( GraphValues.class, r -> true, "NeoGraphValuesRule" );


        private <R extends AlgNode> NeoGraphValuesRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            GraphValues values = (GraphValues) alg;
            return new NeoGraphValues(
                    values.getCluster(),
                    values.getTraitSet().replace( NeoConvention.INSTANCE ),
                    values.getNodes(),
                    values.getEdges(),
                    values.getValues(),
                    values.getRowType() );
        }

    }


    class NeoGraphSortRule extends NeoConverterRule {

        public static NeoGraphSortRule INSTANCE = new NeoGraphSortRule( GraphSort.class, r -> true, "NeoGraphSortRule" );


        private <R extends AlgNode> NeoGraphSortRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            GraphSort sort = (GraphSort) alg;
            return new NeoGraphSort(
                    sort.getCluster(),
                    sort.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( sort.getInput(), NeoConvention.INSTANCE ),
                    sort.collation,
                    sort.offset,
                    sort.fetch );
        }

    }


    class NeoGraphUnwindRule extends NeoConverterRule {

        public static NeoGraphUnwindRule INSTANCE = new NeoGraphUnwindRule( GraphUnwind.class, r -> true, "NeoGraphUnwindRule" );


        private <R extends AlgNode> NeoGraphUnwindRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            GraphUnwind sort = (GraphUnwind) alg;
            return new NeoGraphUnwind(
                    sort.getCluster(),
                    sort.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( sort.getInput(), NeoConvention.INSTANCE ),
                    sort.index,
                    sort.alias );
        }

    }


    class NeoGraphAggregateRule extends NeoConverterRule {

        public static NeoGraphAggregateRule INSTANCE = new NeoGraphAggregateRule( GraphAggregate.class, r -> true, "NeoGraphAggregateRule" );


        private <R extends AlgNode> NeoGraphAggregateRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            GraphAggregate aggregate = (GraphAggregate) alg;
            return new NeoGraphAggregate(
                    aggregate.getCluster(),
                    aggregate.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( aggregate.getInput(), NeoConvention.INSTANCE ),
                    aggregate.indicator,
                    aggregate.getGroupSet(),
                    aggregate.getGroupSets(),
                    aggregate.getAggCallList() );
        }

    }


    class NeoGraphMatchRule extends NeoConverterRule {

        public static NeoGraphMatchRule INSTANCE = new NeoGraphMatchRule( GraphMatch.class, r -> true, "NeoGraphMatchRule" );


        private <R extends AlgNode> NeoGraphMatchRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            GraphMatch match = (GraphMatch) alg;
            return new NeoGraphMatch(
                    match.getCluster(),
                    match.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( match.getInput(), NeoConvention.INSTANCE ),
                    match.getMatches(),
                    match.getNames() );
        }

    }

}
