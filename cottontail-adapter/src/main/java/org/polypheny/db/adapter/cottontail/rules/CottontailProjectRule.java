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

package org.polypheny.db.adapter.cottontail.rules;


import java.util.List;
import org.polypheny.db.adapter.cottontail.CottontailConvention;
import org.polypheny.db.adapter.cottontail.CottontailToEnumerableConverter;
import org.polypheny.db.adapter.cottontail.rel.CottontailProject;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptRuleCall;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.fun.SqlArrayValueConstructor;
import org.polypheny.db.sql.fun.SqlKnnFunction;
import org.polypheny.db.tools.RelBuilderFactory;


public class CottontailProjectRule extends CottontailConverterRule {

    CottontailProjectRule( CottontailConvention out, RelBuilderFactory relBuilderFactory ) {
        super( Project.class, r -> true, Convention.NONE, out, relBuilderFactory, "CottontailProjectRule:" + out.getName() );
    }


    @Override
    public boolean matches( RelOptRuleCall call ) {
        Project project = call.rel( 0 );


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
            } else if ( (e instanceof RexCall) && (((RexCall) e).getOperator() instanceof SqlKnnFunction) ) {
                RexCall rexCall = (RexCall) e;
                SqlKnnFunction knnFunction = (SqlKnnFunction) ((RexCall) e).getOperator();
                if ( !foundKnnFunction ) {
                    RelDataType fieldType = fieldList.get( i ).getType();

                    if ( (CottontailToEnumerableConverter.SUPPORTED_ARRAY_COMPONENT_TYPES.contains( rexCall.getOperands().get( 0 ).getType().getComponentType().getPolyType() ))) {
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

        return ( ( containsInputRefs || containsValueProjects ) && !foundKnnFunction )
                || ( ( containsInputRefs || foundKnnFunction ) && !containsValueProjects);
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
