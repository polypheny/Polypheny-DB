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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.adapter.file.FileAlg;
import org.polypheny.db.adapter.file.FileTranslatableTable;
import org.polypheny.db.adapter.file.Value;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Modify;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.AlgOptTableImpl;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;


public class FileTableModify extends Modify implements FileAlg {

    public FileTableModify( AlgOptCluster cluster, AlgTraitSet traits, AlgOptTable table, CatalogReader catalogReader, AlgNode child, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
        super( cluster, traits, table, catalogReader, child, operation, updateColumnList, sourceExpressionList, flattened );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new FileTableModify(
                getCluster(),
                traitSet,
                getTable(),
                getCatalogReader(),
                AbstractAlgNode.sole( inputs ),
                getOperation(),
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened() );
    }


    @Override
    public void register( AlgOptPlanner planner ) {
        getConvention().register( planner );
    }


    @Override
    public void implement( final FileImplementor implementor ) {
        setOperation( implementor );//do it first, so children know that we have an insert/update/delete
        implementor.visitChild( 0, getInput() );
        FileTranslatableTable fileTable = (FileTranslatableTable) ((AlgOptTableImpl) getTable()).getTable();
        implementor.setFileTable( fileTable );
        if ( getOperation() == Operation.UPDATE ) {
            if ( getSourceExpressionList() != null ) {
                if ( implementor.getUpdates() == null ) {
                    implementor.setUpdates( new ArrayList<>() );
                }
                implementor.getUpdates().clear();
                List<Value> values = new ArrayList<>();
                int i = 0;
                for ( RexNode src : getSourceExpressionList() ) {
                    if ( src instanceof RexLiteral ) {
                        values.add( new Value( implementor.getFileTable().getColumnIdMap().get( getUpdateColumnList().get( i ) ).intValue(), ((RexLiteral) src).getValueForFileCondition(), false ) );
                    } else if ( src instanceof RexDynamicParam ) {
                        values.add( new Value( implementor.getFileTable().getColumnIdMap().get( getUpdateColumnList().get( i ) ).intValue(), ((RexDynamicParam) src).getIndex(), true ) );
                    } else if ( src instanceof RexCall && src.getType().getPolyType() == PolyType.ARRAY ) {
                        values.add( Value.fromArrayRexCall( (RexCall) src ) );
                    } else {
                        throw new RuntimeException( "Unknown element in sourceExpressionList: " + src.toString() );
                    }
                    i++;
                }
                implementor.setUpdates( values );
            }
            //set the columns that should be updated in the projection list
            implementor.project( this.getUpdateColumnList(), null );
        }
    }


    private void setOperation( final FileImplementor implementor ) {
        Operation operation = getOperation();
        switch ( operation ) {
            case INSERT:
                implementor.setOperation( FileImplementor.Operation.INSERT );
                break;
            case UPDATE:
                implementor.setOperation( FileImplementor.Operation.UPDATE );
                break;
            case DELETE:
                implementor.setOperation( FileImplementor.Operation.DELETE );
                break;
            default:
                throw new RuntimeException( "The File adapter does not support " + operation + "operations." );
        }
    }

}
