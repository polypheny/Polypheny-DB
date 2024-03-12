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

import java.util.Optional;
import java.util.function.Predicate;
import org.polypheny.db.adapter.neo4j.NeoConvention;
import org.polypheny.db.adapter.neo4j.NeoEntity;
import org.polypheny.db.adapter.neo4j.NeoToEnumerableConverterRule;
import org.polypheny.db.adapter.neo4j.rules.relational.NeoFilter;
import org.polypheny.db.adapter.neo4j.rules.relational.NeoModify;
import org.polypheny.db.adapter.neo4j.rules.relational.NeoProject;
import org.polypheny.db.adapter.neo4j.rules.relational.NeoValues;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.util.UnsupportedRelFromInsertShuttle;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.types.ModifiableTable;

public interface NeoRules {


    AlgOptRule[] RULES = {
            NeoToEnumerableConverterRule.INSTANCE,
            NeoModifyRule.INSTANCE,
            NeoProjectRule.INSTANCE,
            NeoFilterRule.INSTANCE,
            NeoValuesRule.INSTANCE
    };


    abstract class NeoConverterRule extends ConverterRule {

        protected final Convention out;


        public <R extends AlgNode> NeoConverterRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, Convention.NONE, NeoConvention.INSTANCE, AlgFactories.LOGICAL_BUILDER, description );
            this.out = NeoConvention.INSTANCE;
        }

    }


    class NeoModifyRule extends NeoConverterRule {

        public static NeoModifyRule INSTANCE = new NeoModifyRule( RelModify.class, NeoModifyRule::supports, "NeoModifyRule" );


        private static boolean supports( RelModify<?> modify ) {
            return !modify.isInsert() || !UnsupportedRelFromInsertShuttle.contains( modify );
        }


        private <R extends AlgNode> NeoModifyRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            RelModify<?> modify = (RelModify<?>) alg;

            Optional<ModifiableTable> oModifiableTable = modify.getEntity().unwrap( ModifiableTable.class );
            if ( oModifiableTable.isEmpty() ) {
                return null;
            }
            Optional<NeoEntity> oMongo = modify.getEntity().unwrap( NeoEntity.class );
            return oMongo.map( neoEntity -> new NeoModify(
                    modify.getCluster(),
                    modify.getTraitSet().replace( NeoConvention.INSTANCE ),
                    neoEntity,
                    convert( modify.getInput(), NeoConvention.INSTANCE ),
                    modify.getOperation(),
                    modify.getUpdateColumns(),
                    modify.getSourceExpressions(),
                    modify.isFlattened() ) ).orElse( null );

        }

    }


    class NeoProjectRule extends NeoConverterRule {

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
                    project.getTupleType() );
        }

    }


    class NeoFilterRule extends NeoConverterRule {

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


    class NeoValuesRule extends NeoConverterRule {

        public static NeoValuesRule INSTANCE = new NeoValuesRule( Values.class, r -> true, "NeoValuesRule" );


        private <R extends AlgNode> NeoValuesRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            Values values = (Values) alg;
            return new NeoValues(
                    values.getCluster(),
                    values.getTupleType(),
                    values.tuples,
                    values.getTraitSet().replace( NeoConvention.INSTANCE ) );
        }

    }


}
