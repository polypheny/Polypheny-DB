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

package org.polypheny.db.adapter.file.algebra;


import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.adapter.file.FileAlg;
import org.polypheny.db.adapter.file.FileAlg.FileImplementor.Operation;
import org.polypheny.db.adapter.file.Value;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;


public class FileProject extends Project implements FileAlg {

    public FileProject( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, List<? extends RexNode> projects, AlgDataType rowType ) {
        super( cluster, traits, input, projects, rowType );
    }


    @Override
    public Project copy( AlgTraitSet traitSet, AlgNode input, List<RexNode> projects, AlgDataType rowType ) {
        return new FileProject( getCluster(), traitSet, input, projects, rowType );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public void implement( FileImplementor implementor ) {
        if ( implementor.getOperation() == Operation.INSERT ) {
            // Visit FileValues only if there are RexInputRefs. Else the values will be in the exps field
            // For non-array inserts, there is no FileProject
            Value[] row = new Value[exps.size()];
            Gson gson = new Gson();
            int i = 0;
            boolean containsInputRefs = false;
            for ( RexNode node : exps ) {
                if ( node instanceof RexLiteral ) {
                    row[i] = new Value( i, ((RexLiteral) node).getValueForFileAdapter(), false );
                } else if ( node instanceof RexInputRef ) {
                    if ( containsInputRefs ) {
                        continue;
                    }
                    containsInputRefs = true;
                    implementor.visitChild( 0, getInput() );
                } else if ( node instanceof RexCall ) {
                    RexCall call = (RexCall) node;
                    ArrayList<Object> arrayValues = new ArrayList<>();
                    for ( RexNode node1 : call.getOperands() ) {
                        arrayValues.add( ((RexLiteral) node1).getValueForFileCondition() );
                    }
                    row[i] = new Value( i, gson.toJson( arrayValues ), false );
                } else if ( node instanceof RexDynamicParam ) {
                    row[i] = new Value( i, ((RexDynamicParam) node).getIndex(), true );
                } else {
                    throw new RuntimeException( "Could not implement " + node.getClass().getSimpleName() + " " + node.toString() );
                }
                i++;
            }
            if ( !containsInputRefs ) {
                implementor.addInsertValue( row );
            }
        } else {
            implementor.visitChild( 0, getInput() );
        }
        if ( implementor.getOperation() == Operation.UPDATE ) {
            implementor.setUpdates( Value.getUpdates( exps, implementor ) );
        }
        AlgRecordType rowType = (AlgRecordType) getRowType();
        List<String> fields = new ArrayList<>();

        ArrayList<Integer> mapping = new ArrayList<>();
        boolean inputRefsOnly = true;
        for ( RexNode e : exps ) {
            if ( e instanceof RexInputRef ) {
                mapping.add( Long.valueOf( ((RexInputRef) e).getIndex() ).intValue() );
            } else {
                inputRefsOnly = false;
                break;
            }
        }
        if ( inputRefsOnly ) {
            implementor.project( null, mapping );
        } else {
            for ( AlgDataTypeField field : rowType.getFieldList() ) {
                if ( field.getKey().startsWith( "EXPR$" ) ) {
                    //don't set EXPR-columns in FileImplementor
                    return;
                }
                fields.add( field.getKey() );
            }
            implementor.project( fields, null );
        }
    }

}
