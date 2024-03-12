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

package org.polypheny.db.adapter.file.algebra;


import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.file.FileConvention;
import org.polypheny.db.adapter.file.FileSchema;
import org.polypheny.db.adapter.file.FileTranslatableEntity;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.util.UnsupportedRelFromInsertShuttle;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil.FieldAccessFinder;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.schema.document.DocumentRules;
import org.polypheny.db.schema.types.ModifiableTable;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.util.UnsupportedRexCallVisitor;


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
            super( RelModify.class, FileTableModificationRule::supports, Convention.NONE, out, algBuilderFactory, "FileTableModificationRule:" + out.getName() );
            this.convention = out;
        }


        private static boolean supports( RelModify<?> node ) {
            if ( node.isInsert() && node.containsScan() ) {
                // insert from select is not implemented
                return false;
            }
            if ( node.getSourceExpressions() != null ) {
                FieldAccessFinder fieldAccessFinder = new FieldAccessFinder();
                for ( RexNode node1 : node.getSourceExpressions() ) {
                    node1.accept( fieldAccessFinder );
                    if ( !fieldAccessFinder.getFieldAccessList().isEmpty() ) {
                        return false;
                    }
                }
            }

            if ( node.getSourceExpressions() != null ) {
                return !UnsupportedRexCallVisitor.containsModelItem( node.getSourceExpressions() );
            }
            return true;
        }


        @Override
        public boolean matches( AlgOptRuleCall call ) {
            final RelModify<?> modify = call.alg( 0 );
            if ( modify.getEntity() == null ) {
                // todo insert from select is not correctly implemented
                return false;
            }

            if ( modify.isInsert() && UnsupportedRelFromInsertShuttle.contains( modify ) ) {
                return false;
            }

            convention.setModification( true );
            return true;
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final RelModify<?> modify = (RelModify<?>) alg;
            Optional<ModifiableTable> oModifiableTable = modify.getEntity().unwrap( ModifiableTable.class );

            if ( oModifiableTable.isEmpty() ) {
                return null;
            }
            if ( modify.getEntity().unwrap( FileTranslatableEntity.class ).isEmpty() ) {
                return null;
            }

            final AlgTraitSet traitSet = modify.getTraitSet().replace( convention );
            return new FileTableModify(
                    modify.getCluster(),
                    traitSet,
                    modify.getEntity().unwrap( FileTranslatableEntity.class ).get(),
                    AlgOptRule.convert( modify.getInput(), traitSet ),
                    modify.getOperation(),
                    modify.getUpdateColumns(),
                    modify.getSourceExpressions(),
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
            super( Project.class, p ->
                    !functionInProject( p )
                            && !UnsupportedRexCallVisitor.containsModelItem( p.getProjects() ), Convention.NONE, out, algBuilderFactory, "FileProjectRule:" + out.getName() );
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
            if ( call.alg( 0 ) instanceof LogicalRelProject && !((LogicalRelProject) call.alg( 0 )).getProjects().isEmpty() ) {
                //RexInputRef occurs in a select query, RexLiteral/RexCall/RexDynamicParam occur in insert/update/delete queries
                boolean isSelect = true;
                for ( RexNode node : ((LogicalRelProject) call.alg( 0 )).getProjects() ) {
                    if ( node instanceof RexIndexRef ) {
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
                    project.getTupleType()
            );
        }


        private static boolean functionInProject( Project project ) {
            CheckingFunctionVisitor visitor = new CheckingFunctionVisitor( project );
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
                    values.getTupleType(),
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
            return new FileUnion(
                    union.getCluster(),
                    traitSet,
                    AlgOptRule.convertList( union.getInputs(), convention ),
                    union.all );
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
            return true;//call.alg( 0 ).getInput( 0 ).getConvention() == convention;
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final Filter filter = (Filter) alg;
            final AlgTraitSet traitSet = filter.getTraitSet().replace( convention );
            return new FileFilter(
                    filter.getCluster(),
                    traitSet,
                    convert( filter.getInput(), filter.getInput().getTraitSet().replace( convention ) ),
                    filter.getCondition() );
        }


        private static boolean functionInFilter( Filter filter ) {
            CheckingFunctionVisitor visitor = new CheckingFunctionVisitor( filter );
            for ( RexNode node : filter.getChildExps() ) {
                node.accept( visitor );
                if ( visitor.containsFunction() ) {
                    return true;
                }
            }
            return false;
        }

    }


    @Getter
    private static class CheckingFunctionVisitor extends RexVisitorImpl<Void> {

        @Accessors(fluent = true)
        private boolean containsFunction = false;

        private boolean isProject;


        CheckingFunctionVisitor( AlgNode node ) {
            super( true );
            isProject = node instanceof LogicalRelProject;
        }


        @Override
        public Void visitCall( RexCall call ) {
            if ( !isProject && call.getKind() == Kind.EQUALS ) {
                return super.visitCall( call );
            }
            containsFunction = true;
            return null;
        }

    }

}
