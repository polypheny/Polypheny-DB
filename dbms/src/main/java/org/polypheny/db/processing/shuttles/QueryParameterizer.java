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

package org.polypheny.db.processing.shuttles;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.DataContext.ParameterValue;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.TableModify;
import org.polypheny.db.algebra.logical.*;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.rex.*;
import org.polypheny.db.sql.sql.fun.SqlArrayValueConstructor;
import org.polypheny.db.type.IntervalPolyType;
import org.polypheny.db.type.PolyType;

public class QueryParameterizer extends AlgShuttleImpl implements RexVisitor<RexNode> {

    private final AtomicInteger index;
    @Getter
    private final Map<Integer, List<ParameterValue>> values;
    @Getter
    private final List<AlgDataType> types;


    public QueryParameterizer( int indexStart, List<AlgDataType> parameterRowType ) {
        index = new AtomicInteger( indexStart );
        values = new HashMap<>();
        types = new ArrayList<>( parameterRowType );
    }


    @Override
    public AlgNode visit( LogicalFilter oFilter ) {
        LogicalFilter filter = (LogicalFilter) super.visit( oFilter );
        RexNode condition = filter.getCondition();
        return new LogicalFilter(
                filter.getCluster(),
                filter.getTraitSet(),
                filter.getInput(),
                condition.accept( this ),
                filter.getVariablesSet() );
    }


    @Override
    public AlgNode visit( LogicalProject oProject ) {
        LogicalProject project = (LogicalProject) super.visit( oProject );
        List<RexNode> newProjects = new ArrayList<>();
        for ( RexNode node : oProject.getProjects() ) {
            newProjects.add( node.accept( this ) );
        }
        return new LogicalProject(
                project.getCluster(),
                project.getTraitSet(),
                project.getInput(),
                newProjects,
                project.getRowType() );
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        if ( other instanceof TableModify ) {
            LogicalTableModify modify = (LogicalTableModify) super.visit( other );
            List<RexNode> newSourceExpression = null;
            if ( modify.getSourceExpressionList() != null ) {
                newSourceExpression = new ArrayList<>();
                for ( RexNode node : modify.getSourceExpressionList() ) {
                    newSourceExpression.add( node.accept( this ) );
                }
            }
            AlgNode input = modify.getInput();
            if ( input instanceof LogicalValues ) {
                List<RexNode> projects = new ArrayList<>();
                boolean firstRow = true;
                HashMap<Integer, Integer> idxMapping = new HashMap<>();
                for ( ImmutableList<RexLiteral> node : ((LogicalValues) input).getTuples() ) {
                    int i = 0;
                    for ( RexLiteral literal : node ) {
                        int idx = getIndex(idxMapping, i);
                        AlgDataType type = input.getRowType().getFieldList().get( i ).getValue();
                        if ( firstRow ) {
                            projects.add( new RexDynamicParam( type, idx ) );
                        }
                        addValue(idx, type, input, literal.getValueForQueryParameterizer());
                        i++;
                    }
                    firstRow = false;
                }
                LogicalValues logicalValues = LogicalValues.createOneRow( input.getCluster() );
                input = new LogicalProject(
                        input.getCluster(),
                        input.getTraitSet(),
                        logicalValues,
                        projects,
                        input.getRowType()
                );
            }
            return new LogicalTableModify(
                    modify.getCluster(),
                    modify.getTraitSet(),
                    modify.getTable(),
                    modify.getCatalogReader(),
                    input,
                    modify.getOperation(),
                    modify.getUpdateColumnList(),
                    newSourceExpression,
                    modify.isFlattened() );
        } else if ( other instanceof LogicalModifyCollect ) {
            List<AlgNode> inputs = new ArrayList<>( other.getInputs().size() );
            for ( AlgNode node : other.getInputs() ) {
                inputs.add( visit( node ) );
            }
            return new LogicalModifyCollect(
                    other.getCluster(),
                    other.getTraitSet(),
                    inputs,
                    ((LogicalModifyCollect) other).all
            );
        } else if (other instanceof LogicalProcedureExecution) {
            LogicalProcedureExecution procedureExecution = (LogicalProcedureExecution) other;
            LogicalTableModify logicalTableModify = (LogicalTableModify) procedureExecution.getInput();
            if (!(logicalTableModify.getInput() instanceof LogicalProject)) {
                return logicalTableModify;
            }
            LogicalProject logicalProject = (LogicalProject) logicalTableModify.getInput();
            HashMap<Integer, Integer> idxMapping = new HashMap<>();
            int i = 0;
            for ( RexNode node : logicalProject.getChildExps()) {
                if(!(node instanceof RexNamedDynamicParam)) {
                    continue;
                }
                Object value = visitRexNamedDynamicParam(procedureExecution, (RexNamedDynamicParam) node);
                int idx = getIndex(idxMapping, i);
                AlgDataType type = logicalProject.getRowType().getFieldList().get( i ).getValue();
                addValue(idx, type, logicalProject.getInput(), value);
                i++;
            }
            // return wrapped AlgNode from LogicalProcedureExecution
            return logicalTableModify;
        } else if (other instanceof LogicalTriggerExecution) {
            LogicalTriggerExecution triggerExecution = (LogicalTriggerExecution) other;
            Map<String, Object> insertParameters = extractParameters(triggerExecution);
            ArrayList<AlgNode> parameterizedNodes = insertProcedureParameters(triggerExecution, insertParameters);
            return triggerExecution.copy(triggerExecution.getTraitSet(), parameterizedNodes);
        } else {
            return super.visit( other );
        }
    }

    @NotNull
    private ArrayList<AlgNode> insertProcedureParameters(LogicalTriggerExecution triggerExecution, Map<String, Object> insertParameters) {
        ArrayList<AlgNode> parameterizedNodes = new ArrayList<>();
        for (AlgNode triggerInput : triggerExecution.getInputs()) {
            if(triggerInput instanceof LogicalProcedureExecution) {
                LogicalProcedureExecution logicalProcedureExecution = (LogicalProcedureExecution) triggerInput;
                LogicalProcedureExecution execution = LogicalProcedureExecution.create(logicalProcedureExecution.getInput(), insertParameters);
                AlgNode visit = visit(execution);
                parameterizedNodes.add(visit);
            } else if (triggerInput instanceof LogicalTableModify) {
                LogicalTableModify tableModify = (LogicalTableModify) triggerInput;
                AlgNode visit = visit(tableModify);
                parameterizedNodes.add(visit);
            }
        }
        return parameterizedNodes;
    }

    @NotNull
    private Map<String, Object> extractParameters(LogicalTriggerExecution triggerExecution) {
        LogicalTableModify insertNode = triggerExecution.getModify();
        AlgNode insertInput = insertNode.getInput();
        LogicalValues logicalValues = getLogicalValues(insertInput);
        return addParameter(logicalValues);
    }

    private LogicalValues getLogicalValues(AlgNode insertInput) {
        LogicalValues logicalValues;
        if(insertInput instanceof LogicalValues) {
            // LogicalValues when more than value given
            logicalValues = (LogicalValues) insertInput;
        } else if (insertInput instanceof LogicalProject) {
            // LogicalProject when one value given
            var logicalProject = (LogicalProject) insertInput;
            logicalValues = (LogicalValues) logicalProject.getInput();
        } else {
            throw new RuntimeException("Cannot extract parameters from LogicalTableModify");
        }
        return logicalValues;
    }

    private Map<String, Object> addParameter(LogicalValues logicalValues) {
        Map<String, Object> insertParameters = new HashMap<>();
        for ( ImmutableList<RexLiteral> node : logicalValues.getTuples() ) {
            int i = 0;
            for ( RexLiteral literal : node ) {
                AlgDataTypeField field = logicalValues.getRowType().getFieldList().get(i);
                insertParameters.put(field.getName(), literal.getValueForQueryParameterizer());
                i++;
            }
        }
        return insertParameters;
    }

//    @Override
//    public AlgNode visit(LogicalProcedureExecution procedureExecution) {
//
//    }

    private Object visitRexNamedDynamicParam(LogicalProcedureExecution procedureExecution, RexNamedDynamicParam node) {
        RexNamedDynamicParam dynamicParam = node;
        if(!procedureExecution.hasMapping(dynamicParam.getName().substring(1))) {
            throw new RuntimeException("No parameter defined for argument ´" + dynamicParam.getName() + "´");
        }
        return procedureExecution.getMapping(dynamicParam.getName().substring(1));
    }

    private void addValue(int idx, AlgDataType type, AlgNode logicalProject, Object value) {
        if ( !values.containsKey(idx) ) {
            types.add(type);
            values.put(idx, new ArrayList<>( ((LogicalValues) logicalProject).getTuples().size() ) );
        }
        // TODO(nic): Get proper type see RexLiteral#queryParameterize
        values.get(idx).add( new ParameterValue(idx, type, value) );
    }

    private int getIndex(HashMap<Integer, Integer> idxMapping, int i) {
        int idx;
        if ( !idxMapping.containsKey(i) ) {
            idx = index.getAndIncrement();
            idxMapping.put(i, idx );
        } else {
            idx = idxMapping.get(i);
        }
        return idx;
    }


//    @Override
//    public AlgNode visit(LogicalTriggerExecution triggerExecution) {
//
//        return visitChildren(triggerExecution);
//    }

    //
    // Rex
    //


    @Override
    public RexNode visitInputRef( RexInputRef inputRef ) {
        return inputRef;
    }


    @Override
    public RexNode visitLocalRef( RexLocalRef localRef ) {
        return localRef;
    }


    @Override
    public RexNode visitLiteral( RexLiteral literal ) {
        if ( literal.getType() instanceof IntervalPolyType && !RuntimeConfig.PARAMETERIZE_INTERVALS.getBoolean() ) {
            return literal;
        }
        int i = index.getAndIncrement();
        values.put( i, Collections.singletonList( new ParameterValue( i, literal.getType(), literal.getValueForQueryParameterizer() ) ) );
        types.add( literal.getType() );

        return new RexDynamicParam( literal.getType(), i );
    }


    @Override
    public RexNode visitCall( RexCall call ) {
        if ( call.getKind().belongsTo( Kind.MQL_KIND ) ) {
            return call;
        } else if ( call.op instanceof SqlArrayValueConstructor ) {
            int i = index.getAndIncrement();
            List<Object> list = createListForArrays( call.operands );
            values.put( i, Collections.singletonList( new ParameterValue( i, call.type, list ) ) );
            types.add( call.type );
            return new RexDynamicParam( call.type, i );
        } else {
            List<RexNode> newOperands = new LinkedList<>();
            for ( RexNode operand : call.operands ) {
                if ( operand instanceof RexLiteral && ((RexLiteral) operand).getTypeName() == PolyType.SYMBOL ) {
                    // Do not replace with dynamic param
                    newOperands.add( operand );
                } else {
                    newOperands.add( operand.accept( this ) );
                }
            }
            return new RexCall( call.type, call.op, newOperands );
        }
    }


    private List<Object> createListForArrays( List<RexNode> operands ) {
        List<Object> list = new ArrayList<>( operands.size() );
        for ( RexNode node : operands ) {
            if ( node instanceof RexLiteral ) {
                list.add( ((RexLiteral) node).getValueForQueryParameterizer() );
            } else if ( node instanceof RexCall ) {
                list.add( createListForArrays( ((RexCall) node).operands ) );
            } else {
                throw new RuntimeException( "Invalid array" );
            }
        }
        return list;
    }


    @Override
    public RexNode visitOver( RexOver over ) {
        List<RexNode> newOperands = new LinkedList<>();
        for ( RexNode operand : over.operands ) {
            newOperands.add( operand.accept( this ) );
        }
        return new RexCall( over.type, over.op, newOperands );
    }


    @Override
    public RexNode visitCorrelVariable( RexCorrelVariable correlVariable ) {
        return correlVariable;
    }


    @Override
    public RexNode visitDynamicParam( RexDynamicParam dynamicParam ) {
        return dynamicParam;
    }


    @Override
    public RexNode visitRangeRef( RexRangeRef rangeRef ) {
        return rangeRef;
    }


    @Override
    public RexNode visitFieldAccess( RexFieldAccess fieldAccess ) {
        return fieldAccess;
    }


    @Override
    public RexNode visitSubQuery( RexSubQuery subQuery ) {
        List<RexNode> newOperands = new LinkedList<>();
        for ( RexNode operand : subQuery.operands ) {
            newOperands.add( operand.accept( this ) );
        }
        return subQuery.clone( subQuery.type, newOperands, subQuery.alg.accept( this ) );
    }


    @Override
    public RexNode visitTableInputRef( RexTableInputRef fieldRef ) {
        return fieldRef;
    }


    @Override
    public RexNode visitPatternFieldRef( RexPatternFieldRef fieldRef ) {
        return fieldRef;
    }

}
