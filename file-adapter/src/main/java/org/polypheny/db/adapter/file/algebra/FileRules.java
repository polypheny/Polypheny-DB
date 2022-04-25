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

package org.polypheny.db.adapter.file.algebra;


import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.util.List;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.file.FileConvention;
import org.polypheny.db.adapter.file.FileSchema;
import org.polypheny.db.adapter.file.FileTranslatableTable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.nodes.Function;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.document.DocumentRules;
import org.polypheny.db.tools.AlgBuilderFactory;


@Slf4j
public class FileRules {

    public static List<AlgOptRule> rules( FileConvention out, Method enumeratorMethod, FileSchema fileSchema ) {
        return ImmutableList.of(
                new FileToEnumerableConverterRule( out, AlgFactories.LOGICAL_BUILDER, enumeratorMethod, fileSchema ),
                new FileProjectRule( out, AlgFactories.LOGICAL_BUILDER ),
                new FileValuesRule( out, AlgFactories.LOGICAL_BUILDER ),
                new FileTableModificationRule( out, AlgFactories.LOGICAL_BUILDER ),
                //new FileUnionRule( out, RelFactories.LOGICAL_BUILDER ),
                new FileFilterRule( out, AlgFactories.LOGICAL_BUILDER )
        );
    }


    static class FileTableModificationRule extends ConverterRule {

        protected final FileConvention convention;


        public FileTableModificationRule( FileConvention out, AlgBuilderFactory algBuilderFactory ) {
            super( Modify.class, FileTableModificationRule::supports, Convention.NONE, out, algBuilderFactory, "FileTableModificationRule:" + out.getName() );
            this.convention = out;
        }


        private static boolean supports( Modify node ) {
            if ( node.getSourceExpressionList() != null ) {
                return node.getSourceExpressionList().stream().noneMatch( DocumentRules::containsDocumentUpdate )
                        && node.getSourceExpressionList().stream().noneMatch( UnsupportedRexCallVisitor::containsUnsupportedCall );
            }
            return true;
        }


        @Override
        public boolean matches( AlgOptRuleCall call ) {
            final Modify modify = call.alg( 0 );
            if ( modify.getTable().unwrap( FileTranslatableTable.class ) == null ) {
                return false;
            }
            FileTranslatableTable table = modify.getTable().unwrap( FileTranslatableTable.class );
            /*if ( convention.getFileSchema() != table.getFileSchema() ) {
                return false;
            }*/
            convention.setModification( true );
            return true;
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final Modify modify = (Modify) alg;
            final ModifiableTable modifiableTable = modify.getTable().unwrap( ModifiableTable.class );
            //todo this check might be redundant
            if ( modifiableTable == null ) {
                log.warn( "Returning null during conversion" );
                return null;
            }
            final AlgTraitSet traitSet = modify.getTraitSet().replace( convention );
            return new FileTableModify(
                    modify.getCluster(),
                    traitSet,
                    modify.getTable(),
                    modify.getCatalogReader(),
                    AlgOptRule.convert( modify.getInput(), traitSet ),
                    modify.getOperation(),
                    modify.getUpdateColumnList(),
                    modify.getSourceExpressionList(),
                    modify.isFlattened()
            );
        }

    }


    static class FileToEnumerableConverterRule extends ConverterRule {

        private final Method enumeratorMethod;
        private final FileSchema fileSchema;


        public FileToEnumerableConverterRule( FileConvention convention, AlgBuilderFactory algBuilderFactory, Method enumeratorMethod, FileSchema fileSchema ) {
            super( AlgNode.class, r -> true, convention, EnumerableConvention.INSTANCE, algBuilderFactory, "FileToEnumerableConverterRule:" + convention.getName() );
            this.enumeratorMethod = enumeratorMethod;
            this.fileSchema = fileSchema;
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            AlgTraitSet newTraitSet = alg.getTraitSet().replace( getOutTrait() );
            return new FileToEnumerableConverter( alg.getCluster(), newTraitSet, alg, enumeratorMethod, fileSchema );
        }

    }


    static class FileProjectRule extends ConverterRule {

        protected final FileConvention convention;


        public FileProjectRule( FileConvention out, AlgBuilderFactory algBuilderFactory ) {
            super( Project.class, p -> !functionInProject( p ), Convention.NONE, out, algBuilderFactory, "FileProjectRule:" + out.getName() );
            this.convention = out;
        }


        /**
         * Needed for the {@link FileUnionRule}.
         * A FileUnion should only be generated during a multi-insert with arrays.
         * Since matches() seems to be called from bottom-up, the matches() method of the the FileUnionRule is called before the matches() method of the FileTableModificationRule
         * Therefore, the information if the query is a modify or select, has already to be determined here
         */
        @Override
        public boolean matches( AlgOptRuleCall call ) {
            if ( call.alg( 0 ) instanceof LogicalProject && ((LogicalProject) call.alg( 0 )).getProjects().size() > 0 ) {
                //RexInputRef occurs in a select query, RexLiteral/RexCall/RexDynamicParam occur in insert/update/delete queries
                boolean isSelect = true;
                for ( RexNode node : ((LogicalProject) call.alg( 0 )).getProjects() ) {
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
        public AlgNode convert( AlgNode alg ) {
            final Project project = (Project) alg;
            final AlgTraitSet traitSet = project.getTraitSet().replace( convention );

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


        FileValuesRule( FileConvention out, AlgBuilderFactory algBuilderFactory ) {
            super( Values.class, r -> true, Convention.NONE, out, algBuilderFactory, "FileValuesRule:" + out.getName() );
            this.convention = out;
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            Values values = (Values) alg;
            return new FileValues(
                    values.getCluster(),
                    values.getRowType(),
                    values.getTuples(),
                    values.getTraitSet().replace( convention ) );
        }

    }


    static class FileUnionRule extends ConverterRule {

        FileConvention convention;


        public FileUnionRule( FileConvention out, AlgBuilderFactory algBuilderFactory ) {
            super( Union.class, r -> true, Convention.NONE, out, algBuilderFactory, "FileUnionRule:" + out.getName() );
            this.convention = out;
        }


        /**
         * The FileUnion is needed for insert statements with arrays
         * see {@link FileProjectRule#matches}
         */
        @Override
        public boolean matches( AlgOptRuleCall call ) {
            return this.convention.isModification();
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final Union union = (Union) alg;
            final AlgTraitSet traitSet = union.getTraitSet().replace( convention );
            return new FileUnion( union.getCluster(), traitSet, AlgOptRule.convertList( union.getInputs(), convention ), union.all );
        }

    }


    static class FileFilterRule extends ConverterRule {

        FileConvention convention;


        public FileFilterRule( FileConvention out, AlgBuilderFactory algBuilderFactory ) {
            super( Filter.class, f -> !functionInFilter( f ) && !DocumentRules.containsDocument( f ), Convention.NONE, out, algBuilderFactory, "FileFilterRule:" + out.getName() );
            this.convention = out;
        }


        /**
         * The FileUnion is needed for insert statements with arrays
         * see {@link FileProjectRule#matches}
         */
        @Override
        public boolean matches( AlgOptRuleCall call ) {
            return call.alg( 0 ).getInput( 0 ).getConvention() == convention;
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final Filter filter = (Filter) alg;
            final AlgTraitSet traitSet = filter.getTraitSet().replace( convention );
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


    private static class UnsupportedRexCallVisitor extends RexVisitorImpl<Void> {

        @Getter
        boolean containsUnsupportedRexCall = false;


        protected UnsupportedRexCallVisitor() {
            super( true );
        }


        @Override
        public Void visitCall( RexCall call ) {
            if ( call.op.getOperatorName() != OperatorName.ARRAY_VALUE_CONSTRUCTOR ) {
                containsUnsupportedRexCall = true;
            }
            return super.visitCall( call );
        }


        static boolean containsUnsupportedCall( RexNode node ) {
            UnsupportedRexCallVisitor visitor = new UnsupportedRexCallVisitor();
            node.accept( visitor );
            return visitor.containsUnsupportedRexCall;
        }

    }


    private static class CheckingFunctionVisitor extends RexVisitorImpl<Void> {

        @Getter
        @Accessors(fluent = true)
        private boolean containsFunction = false;


        CheckingFunctionVisitor() {
            super( true );
        }


        @Override
        public Void visitCall( RexCall call ) {
            Operator operator = call.getOperator();
            if ( operator instanceof Function ) {
                containsFunction = true;
            }
            return super.visitCall( call );
        }

    }

}
