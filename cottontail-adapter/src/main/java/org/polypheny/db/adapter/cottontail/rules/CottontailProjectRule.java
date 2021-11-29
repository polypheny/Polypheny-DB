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

package org.polypheny.db.adapter.cottontail.rules;


import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.polypheny.db.adapter.cottontail.CottontailConvention;
import org.polypheny.db.adapter.cottontail.CottontailToEnumerableConverter;
import org.polypheny.db.adapter.cottontail.rel.CottontailProject;
import org.polypheny.db.languages.sql.fun.SqlArrayValueConstructor;
import org.polypheny.db.languages.sql.fun.SqlDistanceFunction;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.plan.volcano.RelSubset;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.document.DocumentRules;
import org.polypheny.db.tools.RelBuilderFactory;
import org.polypheny.db.type.PolyType;


public class CottontailProjectRule extends CottontailConverterRule {

    CottontailProjectRule( CottontailConvention out, RelBuilderFactory relBuilderFactory ) {
        super( Project.class, p -> !DocumentRules.containsDocument( p ), Convention.NONE, out, relBuilderFactory, "CottontailProjectRule:" + out.getName() );
    }


    private static boolean containsInnerProject( Project project ) {
        List<RelNode> rels = ((RelSubset) project.getInput()).getRelList();
        Set<RelNode> alreadyChecked = new HashSet<>();
        Deque<RelNode> innerLevel = new LinkedList<>( rels );

        while ( !innerLevel.isEmpty() ) {
            RelNode relNode = innerLevel.pop();
            alreadyChecked.add( relNode );
            if ( relNode instanceof Project ) {
                return true;
            } else {
                for ( RelNode innerNode : relNode.getInputs() ) {
                    if ( innerNode instanceof RelSubset ) {
                        for ( RelNode possibleNewRel : ((RelSubset) innerNode).getRelList() ) {
                            if ( !alreadyChecked.contains( possibleNewRel ) ) {
                                innerLevel.addLast( possibleNewRel );
                            }
                        }
                    }
                }
            }
        }
        return false;
    }


    @Override
    public boolean matches( RelOptRuleCall call ) {
        Project project = call.rel( 0 );
        if ( containsInnerProject( project ) ) {
            return super.matches( call );
        }

        boolean onlyInputRefs = true;
        boolean containsInputRefs = false;
        boolean valueProject = true;
        boolean containsValueProjects = false;
        boolean foundKnnFunction = false;

        List<RexNode> projects = project.getProjects();
        List<RelDataTypeField> fieldList = project.getRowType().getFieldList();
        for ( int i = 0, projectsSize = projects.size(); i < projectsSize; i++ ) {
            RexNode e = projects.get( i );

            if ( e instanceof RexInputRef ) {
                valueProject = false;
                containsInputRefs = true;
            } else if ( (e instanceof RexLiteral) || (e instanceof RexDynamicParam) || ((e instanceof RexCall) && (((RexCall) e).getOperator() instanceof SqlArrayValueConstructor)) ) {
                onlyInputRefs = false;
                containsValueProjects = true;
            } else if ( (e instanceof RexCall) && (((RexCall) e).getOperator() instanceof SqlDistanceFunction) ) {
                RexCall rexCall = (RexCall) e;
                SqlDistanceFunction knnFunction = (SqlDistanceFunction) ((RexCall) e).getOperator();
                if ( !foundKnnFunction ) {
                    RelDataType fieldType = fieldList.get( i ).getType();

                    if ( rexCall.getOperands().size() <= 3 ) {
                        // No optimisation parameter, thus we cannot push this function down!
                        return false;
                    } else if ( rexCall.getOperands().size() == 4 ) {
                        if ( rexCall.getOperands().get( 3 ).getType().getPolyType() != PolyType.INTEGER ) {
                            // 4th argument is not an integer, thus it's not the optimisation parameter.
                            // This means we cannot push down this knn call.
                            return false;
                        }
                    }

                    if ( (CottontailToEnumerableConverter.SUPPORTED_ARRAY_COMPONENT_TYPES.contains( rexCall.getOperands().get( 0 ).getType().getComponentType().getPolyType() )) ) {
                        foundKnnFunction = true;
                        valueProject = false;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                return false;
//                onlyInputRefs = false;
//                valueProject = false;
            }
            /*if ( !(e instanceof RexInputRef) && !(e instanceof RexLiteral) ) {
                if ( !((e instanceof RexCall) && (((RexCall) e).getOperator() instanceof SqlArrayValueConstructor)) ) {
                    return false;
                }
            }*/
        }

        if ( foundKnnFunction ) {
            return false;
        }

        return ((containsInputRefs || containsValueProjects) && !foundKnnFunction)
                || ((containsInputRefs || foundKnnFunction) && !containsValueProjects);
//        return onlyInputRefs || valueProject;

    }


    @Override
    public RelNode convert( RelNode rel ) {
        final Project project = (Project) rel;
        final RelTraitSet traitSet = project.getTraitSet().replace( out );

        boolean arrayValueProject = true;
        for ( RexNode e : project.getProjects() ) {
            if ( !((e instanceof RexCall) && (((RexCall) e).getOperator() instanceof SqlArrayValueConstructor)) && !(e instanceof RexLiteral) && !(e instanceof RexDynamicParam) ) {
                arrayValueProject = false;
            }
        }

        return new CottontailProject(
                project.getCluster(),
                traitSet,
                convert( project.getInput(), project.getInput().getTraitSet().replace( out ) ),
                project.getProjects(),
                project.getRowType(),
                arrayValueProject );
    }

}
