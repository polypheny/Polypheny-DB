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


import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.adapter.file.FileRel;
import org.polypheny.db.adapter.file.FileRel.FileImplementor.Operation;
import org.polypheny.db.adapter.file.Value;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelRecordType;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;


public class FileProject extends Project implements FileRel {

    public FileProject( RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects, RelDataType rowType ) {
        super( cluster, traits, input, projects, rowType );
    }

    @Override
    public Project copy( RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType ) {
        return new FileProject( getCluster(), traitSet, input, projects, rowType );
    }

    @Override
    public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }

    @Override
    public void implement( FileImplementor implementor ) {
        if ( implementor.getOperation() == Operation.INSERT ) {
            //don't visit FileValues, the values are in the exps field
            //for non-array inserts, there is no FileProject
            Value[] row = new Value[exps.size()];
            Gson gson = new Gson();
            int i = 0;
            for ( RexNode node : exps ) {
                if ( node instanceof RexLiteral ) {
                    row[i] = new Value( i, ((RexLiteral) node).getValueForFileAdapter(), false );
                } else if ( node instanceof RexCall ) {
                    RexCall call = (RexCall) node;
                    ArrayList<Object> arrayValues = new ArrayList<>();
                    for ( RexNode node1 : call.getOperands() ) {
                        arrayValues.add( ((RexLiteral) node1).getValueForFileCondition() );
                    }
                    row[i] = new Value( i, gson.toJson( arrayValues ), false );
                } else if ( node instanceof RexDynamicParam ) {
                    row[i] = new Value( i, ((RexDynamicParam) node).getIndex(), true );
                }
                i++;
            }
            implementor.addInsertValue( row );
        } else {
            implementor.visitChild( 0, getInput() );
        }
        if ( implementor.getOperation() == Operation.UPDATE ) {
            implementor.setUpdates( Value.getUpdates( exps, implementor ) );
        }
        RelRecordType rowType = (RelRecordType) getRowType();
        List<String> fields = new ArrayList<>();
        for ( RelDataTypeField field : rowType.getFieldList() ) {
            if ( field.getKey().startsWith( "EXPR$" ) ) {
                //don't set EXPR-columns in FileImplementor
                return;
            }
            fields.add( field.getKey() );
        }
        implementor.project( fields );
    }
}
