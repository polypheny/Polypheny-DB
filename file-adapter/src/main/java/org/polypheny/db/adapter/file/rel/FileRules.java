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

package org.polypheny.db.adapter.file.rel;


import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.file.FileConvention;
import org.polypheny.db.adapter.file.FileTranslatableTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterRule;
import org.polypheny.db.rel.core.Filter;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.Union;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.tools.RelBuilderFactory;


@Slf4j
public class FileRules {

    public static List<RelOptRule> rules( FileConvention out ) {
        return ImmutableList.of(
                new FileToEnumerableConverterRule( out, RelFactories.LOGICAL_BUILDER ),
                new FileProjectRule( out, RelFactories.LOGICAL_BUILDER ),
                new FileValuesRule( out, RelFactories.LOGICAL_BUILDER ),
                new FileTableModificationRule( out, RelFactories.LOGICAL_BUILDER ),
                new FileUnionRule( out, RelFactories.LOGICAL_BUILDER ),
                new FileFilterRule( out, RelFactories.LOGICAL_BUILDER )
        );
    }


    static class FileTableModificationRule extends ConverterRule {

        protected final FileConvention convention;


        public FileTableModificationRule( FileConvention out, RelBuilderFactory relBuilderFactory ) {
            super( TableModify.class, r -> true, Convention.NONE, out, relBuilderFactory, "FileTableModificationRule:" + out.getName() );
            this.convention = out;
        }


        @Override
        public boolean matches( RelOptRuleCall call ) {
            final TableModify tableModify = call.rel( 0 );
            if ( tableModify.getTable().unwrap( FileTranslatableTable.class ) == null ) {
                return false;
            }
            FileTranslatableTable table = tableModify.getTable().unwrap( FileTranslatableTable.class );
            if ( convention.getFileSchema() != table.getFileSchema() ) {
                return false;
            }
            convention.setModification( true );
            return true;
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final TableModify modify = (TableModify) rel;
            final ModifiableTable modifiableTable = modify.getTable().unwrap( ModifiableTable.class );
            //todo this check might be redundant
            if ( modifiableTable == null ) {
                log.warn( "Returning null during conversion" );
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


    static class FileToEnumerableConverterRule extends ConverterRule {

        public FileToEnumerableConverterRule( FileConvention convention, RelBuilderFactory relBuilderFactory ) {
            super( RelNode.class, r -> true, convention, EnumerableConvention.INSTANCE, relBuilderFactory, "FileToEnumerableConverterRule:" + convention.getName() );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            RelTraitSet newTraitSet = rel.getTraitSet().replace( getOutTrait() );
            return new FileToEnumerableConverter( rel.getCluster(), newTraitSet, rel );
        }

    }


    static class FileProjectRule extends ConverterRule {

        protected final FileConvention convention;


        public FileProjectRule( FileConvention out, RelBuilderFactory relBuilderFactory ) {
            super( Project.class, p -> !functionInProject( p ), Convention.NONE, out, relBuilderFactory, "FileProjectRule:" + out.getName() );
            this.convention = out;
        }


        /**
         * Needed for the {@link FileUnionRule}.
         * A FileUnion should only be generated during a multi-insert with arrays.
         * Since matches() seems to be called from bottom-up, the matches() method of the the FileUnionRule is called before the matches() method of the FileTableModificationRule
         * Therefore, the information if the query is a modify or select, has already to be determined here
         */
        @Override
        public boolean matches( RelOptRuleCall call ) {
            if ( call.rel( 0 ) instanceof LogicalProject && ((LogicalProject) call.rel( 0 )).getProjects().size() > 0 ) {
                //RexInputRef occurs in a select query, RexLiteral/RexCall/RexDynamicParam occur in insert/update/delete queries
                boolean isSelect = true;
                for ( RexNode node : ((LogicalProject) call.rel( 0 )).getProjects() ) {
                    if ( node instanceof RexInputRef ) {
                        continue;
                    }
                    isSelect = false;
                    break;
                }
                if ( !isSelect ) {
                    convention.setModification( true );
                }
            }
            return super.matches( call );
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


        private static boolean functionInProject( Project project ) {
            CheckingFunctionVisitor visitor = new CheckingFunctionVisitor();
            for ( RexNode node : project.getChildExps() ) {
                node.accept( visitor );
                if ( visitor.containsFunction() ) {
                    return true;
                }
            }
            return false;
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


    static class FileUnionRule extends ConverterRule {

        FileConvention convention;


        public FileUnionRule( FileConvention out, RelBuilderFactory relBuilderFactory ) {
            super( Union.class, r -> true, Convention.NONE, out, relBuilderFactory, "FileUnionRule:" + out.getName() );
            this.convention = out;
        }


        /**
         * The FileUnion is needed for insert statements with arrays
         * see {@link FileProjectRule#matches}
         */
        @Override
        public boolean matches( RelOptRuleCall call ) {
            return this.convention.isModification();
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final Union union = (Union) rel;
            final RelTraitSet traitSet = union.getTraitSet().replace( convention );
            return new FileUnion( union.getCluster(), traitSet, RelOptRule.convertList( union.getInputs(), convention ), union.all );
        }

    }


    static class FileFilterRule extends ConverterRule {

        FileConvention convention;


        public FileFilterRule( FileConvention out, RelBuilderFactory relBuilderFactory ) {
            super( Filter.class, f -> !functionInFilter( f ), Convention.NONE, out, relBuilderFactory, "FileFilterRule:" + out.getName() );
            this.convention = out;
        }


        /**
         * The FileUnion is needed for insert statements with arrays
         * see {@link FileProjectRule#matches}
         */
        @Override
        public boolean matches( RelOptRuleCall call ) {
            return call.rel( 0 ).getInput( 0 ).getConvention() == convention;
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final Filter filter = (Filter) rel;
            final RelTraitSet traitSet = filter.getTraitSet().replace( convention );
            return new FileFilter( filter.getCluster(), traitSet, filter.getInput(), filter.getCondition() );
        }


        private static boolean functionInFilter( Filter filter ) {
            CheckingFunctionVisitor visitor = new CheckingFunctionVisitor();
            for ( RexNode node : filter.getChildExps() ) {
                node.accept( visitor );
                if ( visitor.containsFunction() ) {
                    return true;
                }
            }
            return false;
        }

    }


    private static class CheckingFunctionVisitor extends RexVisitorImpl<Void> {

        private boolean containsFunction = false;


        CheckingFunctionVisitor() {
            super( true );
        }


        public boolean containsFunction() {
            return containsFunction;
        }


        @Override
        public Void visitCall( RexCall call ) {
            SqlOperator operator = call.getOperator();
            if ( operator instanceof SqlFunction ) {
                containsFunction = true;
            }
            return super.visitCall( call );
        }

    }

}
