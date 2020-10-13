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

package org.polypheny.db.adapter.file.rel;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.file.FileConvention;
import org.polypheny.db.adapter.file.FileTranslatableTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterRule;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.tools.RelBuilderFactory;


public class FileRules {

    public static List<RelOptRule> rules ( FileConvention out ) {
        return ImmutableList.of(
                new FileToEnumerableConverterRule( out, RelFactories.LOGICAL_BUILDER),
                new FileProjectRule( out, RelFactories.LOGICAL_BUILDER ),
                new FileValuesRule( out, RelFactories.LOGICAL_BUILDER ),
                new FileTableModificationRule( out, RelFactories.LOGICAL_BUILDER )
        );
    }

    static class FileTableModificationRule extends ConverterRule {

        protected final Convention convention;

        public FileTableModificationRule( FileConvention out, RelBuilderFactory relBuilderFactory ) {
            super( TableModify.class, r -> true, Convention.NONE, out, relBuilderFactory, "FileTableModificationRule:" + out.getName() );
            this.convention = out;
        }

        @Override
        public boolean matches( RelOptRuleCall call ) {
            final TableModify tableModify = call.rel( 0 );
            if ( tableModify.getTable().unwrap( FileTranslatableTable.class ) != null ) {
                return true;
            }
            System.out.println("Did not convert TableModify into FileTranslatableTable");
            return false;
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final TableModify modify = (TableModify) rel;
            final ModifiableTable modifiableTable = modify.getTable().unwrap( ModifiableTable.class );
            //todo this check might be redundant
            if ( modifiableTable == null ) {
                System.out.println("Returning null during conversion");
                return null;
            }
            final RelTraitSet traitSet = modify.getTraitSet().replace( convention );
            return new FileTableModify(
                    modify.getCluster(),
                    traitSet,
                    modify.getTable(),
                    modify.getCatalogReader(),
                    RelOptRule.convert( modify.getInput(), traitSet ),
                    modify.getOperation(),
                    modify.getUpdateColumnList(),
                    modify.getSourceExpressionList(),
                    modify.isFlattened()
            );
        }
    }

    static class FileToEnumerableConverterRule extends ConverterRule{

        public FileToEnumerableConverterRule( FileConvention convention, RelBuilderFactory relBuilderFactory ) {
            super( RelNode.class, r -> true, convention, EnumerableConvention.INSTANCE, relBuilderFactory, "FileToEnumerableConverterRule:" + convention.getName() );
        }

        @Override
        public RelNode convert( RelNode rel ) {
            RelTraitSet newTraitSet = rel.getTraitSet().replace( getOutTrait() );
            return new FileToEnumerableConverter( rel.getCluster(), newTraitSet, rel );
        }
    }

    static class FileProjectRule extends ConverterRule{

        protected final Convention convention;

        public FileProjectRule( FileConvention out, RelBuilderFactory relBuilderFactory ) {
            super( Project.class, r -> true, Convention.NONE, out, relBuilderFactory, "FileProjectRule:" + out.getName() );
            this.convention = out;
        }

        @Override
        public RelNode convert( RelNode rel ) {
            final Project project = (Project) rel;
            final RelTraitSet traitSet = project.getTraitSet().replace( convention );

            return new FileProject(
                    project.getCluster(),
                    traitSet,
                    convert( project.getInput(), project.getInput().getTraitSet().replace( convention ) ),
                    project.getProjects(),
                    project.getRowType()
            );
        }
    }

    static class FileValuesRule extends ConverterRule {

        FileConvention convention;

        FileValuesRule( FileConvention out, RelBuilderFactory relBuilderFactory ) {
            super( Values.class, r -> true, Convention.NONE, out, relBuilderFactory, "FileValuesRule:" + out.getName() );
            this.convention = out;
        }


        @Override
        public RelNode convert( RelNode rel ) {
            Values values = (Values) rel;
            return new FileValues(
                    values.getCluster(),
                    values.getRowType(),
                    values.getTuples(),
                    values.getTraitSet().replace( convention ) );
        }
    }

}
