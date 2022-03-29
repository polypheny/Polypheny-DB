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

package org.polypheny.db.adapter.neo4j.rules.graph;

import java.util.function.Predicate;
import lombok.Getter;
import org.polypheny.db.adapter.neo4j.NeoConvention;
import org.polypheny.db.adapter.neo4j.NeoToEnumerableConverterRule;
import org.polypheny.db.adapter.neo4j.rules.NeoFilter;
import org.polypheny.db.adapter.neo4j.rules.NeoModify;
import org.polypheny.db.adapter.neo4j.rules.NeoProject;
import org.polypheny.db.adapter.neo4j.rules.NeoValues;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.logical.graph.GraphModify;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.Convention;

public class NeoRules {

    @Getter
    public static final AlgOptRule[] RULES = {
            NeoToEnumerableConverterRule.INSTANCE,
            NeoGraphModifyRule.INSTANCE,
            NeoProjectRule.INSTANCE,
            NeoFilterRule.INSTANCE,
            NeoValuesRule.INSTANCE
    };


    private abstract static class NeoConverterRule extends ConverterRule {

        protected final Convention out;


        public <R extends AlgNode> NeoConverterRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, Convention.NONE, NeoConvention.INSTANCE, AlgFactories.LOGICAL_BUILDER, description );
            this.out = NeoConvention.INSTANCE;
        }

    }


    public static class NeoGraphModifyRule extends NeoConverterRule {

        public static NeoGraphModifyRule INSTANCE = new NeoGraphModifyRule( GraphModify.class, r -> true, "NeoGraphModifyRule" );


        private <R extends AlgNode> NeoGraphModifyRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            GraphModify modify = (GraphModify) alg;
            return new NeoModify(
                    modify.getCluster(),
                    modify.getTraitSet().replace( NeoConvention.INSTANCE ),
                    modify.getTable(),
                    modify.getCatalogReader(),
                    convert( modify.getInput(), NeoConvention.INSTANCE ),
                    modify.getOperation(),
                    modify.getUpdateColumnList(),
                    modify.getSourceExpressionList(),
                    modify.isFlattened() );
        }

    }


    public static class NeoProjectRule extends NeoConverterRule {

        public static NeoProjectRule INSTANCE = new NeoProjectRule( Project.class, NeoUtil::supports, "NeoProjectRule" );


        private <R extends AlgNode> NeoProjectRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            Project project = (Project) alg;
            return new NeoProject(
                    alg.getCluster(),
                    alg.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( project.getInput(), NeoConvention.INSTANCE ),
                    project.getProjects(),
                    project.getRowType() );
        }

    }


    public static class NeoFilterRule extends NeoRules.NeoConverterRule {

        public static NeoFilterRule INSTANCE = new NeoFilterRule( Filter.class, NeoUtil::supports, "NeoFilterRule" );


        private <R extends AlgNode> NeoFilterRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public NeoFilter convert( AlgNode alg ) {
            Filter filter = (Filter) alg;
            return new NeoFilter(
                    filter.getCluster(),
                    filter.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( filter.getInput(), NeoConvention.INSTANCE ),
                    filter.getCondition() );
        }

    }


    public static class NeoValuesRule extends NeoConverterRule {

        public static NeoValuesRule INSTANCE = new NeoValuesRule( Values.class, r -> true, "NeoValuesRule" );


        private <R extends AlgNode> NeoValuesRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            Values values = (Values) alg;
            return new NeoValues(
                    values.getCluster(),
                    values.getRowType(),
                    values.tuples,
                    values.getTraitSet().replace( NeoConvention.INSTANCE ) );
        }

    }

}
