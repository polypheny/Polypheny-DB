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

package org.polypheny.db.adapter.neo4j.rules;

import java.util.function.Predicate;
import org.polypheny.db.adapter.neo4j.NeoConvention;
import org.polypheny.db.adapter.neo4j.NeoGraph;
import org.polypheny.db.adapter.neo4j.NeoToEnumerableConverterRule;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgAggregate;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgFilter;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgMatch;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgModify;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgProject;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgSort;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgUnwind;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgValues;
import org.polypheny.db.adapter.neo4j.util.NeoUtil.NeoSupportVisitor;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.lpg.LpgAggregate;
import org.polypheny.db.algebra.core.lpg.LpgFilter;
import org.polypheny.db.algebra.core.lpg.LpgMatch;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.core.lpg.LpgProject;
import org.polypheny.db.algebra.core.lpg.LpgSort;
import org.polypheny.db.algebra.core.lpg.LpgUnwind;
import org.polypheny.db.algebra.core.lpg.LpgValues;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;

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

        public static NeoGraphModifyRule INSTANCE = new NeoGraphModifyRule( LpgModify.class, r -> true, NeoGraphModifyRule.class.getSimpleName() );


        private <R extends AlgNode> NeoGraphModifyRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LpgModify<?> mod = (LpgModify<?>) alg;

            if ( !(mod.entity instanceof NeoGraph) ) {
                return null;
            }

            LpgModify<NeoGraph> modify = (LpgModify<NeoGraph>) alg;
            return new NeoLpgModify(
                    modify.getCluster(),
                    modify.getTraitSet().replace( NeoConvention.INSTANCE ),
                    modify.getEntity(),
                    convert( modify.getInput(), NeoConvention.INSTANCE ),
                    modify.operation,
                    modify.ids,
                    modify.operations );
        }

    }

    static boolean supports( LpgProject r ) {
        NeoSupportVisitor visitor = new NeoSupportVisitor();
        for ( RexNode project : r.getProjects() ) {
            project.accept( visitor );
        }
        return visitor.isSupports();
    }


    class NeoGraphProjectRule extends NeoConverterRule {

        public static NeoGraphProjectRule INSTANCE = new NeoGraphProjectRule( LpgProject.class, NeoGraphRules::supports, "NeoGraphProjectRule" );


        private <R extends AlgNode> NeoGraphProjectRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LpgProject project = (LpgProject) alg;
            return new NeoLpgProject(
                    alg.getCluster(),
                    alg.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( project.getInput(), NeoConvention.INSTANCE ),
                    project.getNames(),
                    project.getProjects() );
        }

    }


    class NeoGraphFilterRule extends NeoConverterRule {

        public static NeoGraphFilterRule INSTANCE = new NeoGraphFilterRule( LpgFilter.class, r -> true, "NeoGraphFilterRule" );


        private <R extends AlgNode> NeoGraphFilterRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public LpgFilter convert( AlgNode alg ) {
            LpgFilter filter = (LpgFilter) alg;
            return new NeoLpgFilter(
                    filter.getCluster(),
                    filter.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( filter.getInput(), NeoConvention.INSTANCE ),
                    filter.getCondition() );
        }

    }


    class NeoGraphValuesRule extends NeoConverterRule {

        public static NeoGraphValuesRule INSTANCE = new NeoGraphValuesRule( LpgValues.class, r -> true, "NeoGraphValuesRule" );


        private <R extends AlgNode> NeoGraphValuesRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LpgValues values = (LpgValues) alg;
            return new NeoLpgValues(
                    values.getCluster(),
                    values.getTraitSet().replace( NeoConvention.INSTANCE ),
                    values.getNodes(),
                    values.getEdges(),
                    values.getValues(),
                    values.getTupleType() );
        }

    }


    class NeoGraphSortRule extends NeoConverterRule {

        public static NeoGraphSortRule INSTANCE = new NeoGraphSortRule( LpgSort.class, r -> true, "NeoGraphSortRule" );


        private <R extends AlgNode> NeoGraphSortRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LpgSort sort = (LpgSort) alg;
            return new NeoLpgSort(
                    sort.getCluster(),
                    sort.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( sort.getInput(), NeoConvention.INSTANCE ),
                    sort.collation,
                    sort.offset,
                    sort.fetch );
        }

    }


    class NeoGraphUnwindRule extends NeoConverterRule {

        public static NeoGraphUnwindRule INSTANCE = new NeoGraphUnwindRule( LpgUnwind.class, r -> true, "NeoGraphUnwindRule" );


        private <R extends AlgNode> NeoGraphUnwindRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LpgUnwind sort = (LpgUnwind) alg;
            return new NeoLpgUnwind(
                    sort.getCluster(),
                    sort.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( sort.getInput(), NeoConvention.INSTANCE ),
                    sort.index,
                    sort.alias );
        }

    }


    class NeoGraphAggregateRule extends NeoConverterRule {

        public static NeoGraphAggregateRule INSTANCE = new NeoGraphAggregateRule( LpgAggregate.class, r -> true, "NeoGraphAggregateRule" );


        private <R extends AlgNode> NeoGraphAggregateRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LpgAggregate aggregate = (LpgAggregate) alg;
            return new NeoLpgAggregate(
                    aggregate.getCluster(),
                    aggregate.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( aggregate.getInput(), NeoConvention.INSTANCE ),
                    aggregate.groups,
                    aggregate.aggCalls,
                    aggregate.getTupleType() );
        }

    }


    class NeoGraphMatchRule extends NeoConverterRule {

        public static NeoGraphMatchRule INSTANCE = new NeoGraphMatchRule( LpgMatch.class, r -> true, NeoGraphMatchRule.class.getSimpleName() );


        private <R extends AlgNode> NeoGraphMatchRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LpgMatch match = (LpgMatch) alg;
            return new NeoLpgMatch(
                    match.getCluster(),
                    match.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( match.getInput(), NeoConvention.INSTANCE ),
                    match.getMatches(),
                    match.getNames() );
        }

    }

}
