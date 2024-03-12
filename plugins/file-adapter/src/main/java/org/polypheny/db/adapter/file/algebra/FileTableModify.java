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


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.adapter.file.FileAlg;
import org.polypheny.db.adapter.file.FileTranslatableEntity;
import org.polypheny.db.adapter.file.Value;
import org.polypheny.db.adapter.file.Value.DynamicValue;
import org.polypheny.db.adapter.file.Value.LiteralValue;
import org.polypheny.db.adapter.file.util.FileUtil;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;


public class FileTableModify extends RelModify<FileTranslatableEntity> implements FileAlg {

    public FileTableModify( AlgCluster cluster, AlgTraitSet traits, FileTranslatableEntity table, AlgNode child, Operation operation, List<String> updateColumnList, List<? extends RexNode> sourceExpressionList, boolean flattened ) {
        super( cluster, traits, table, child, operation, updateColumnList, sourceExpressionList, flattened );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new FileTableModify(
                getCluster(),
                traitSet,
                entity,
                AbstractAlgNode.sole( inputs ),
                getOperation(),
                getUpdateColumns(),
                getSourceExpressions(),
                isFlattened() );
    }


    @Override
    public void register( AlgPlanner planner ) {
        getConvention().register( planner );
    }


    @Override
    public void implement( final FileImplementor implementor ) {
        setOperation( implementor );//do it first, so children know that we have an insert/update/delete
        implementor.visitChild( 0, getInput() );

        implementor.setFileTable( entity );
        if ( getOperation() == Operation.UPDATE ) {
            if ( getSourceExpressions() != null ) {
                if ( implementor.getUpdates() == null ) {
                    implementor.setUpdates( new ArrayList<>() );
                }
                implementor.getUpdates().clear();
                List<Value> values = new ArrayList<>();
                int i = 0;
                for ( RexNode src : getSourceExpressions() ) {
                    if ( src instanceof RexLiteral ) {
                        String logicalName = getUpdateColumns().get( i );
                        AlgDataTypeField field = entity.getTupleType().getField( logicalName, false, false );
                        values.add( new LiteralValue( Math.toIntExact( field.getId() ), ((RexLiteral) src).value ) );
                    } else if ( src instanceof RexDynamicParam ) {
                        String logicalName = getUpdateColumns().get( i );
                        AlgDataTypeField field = entity.getTupleType().getField( logicalName, false, false );
                        values.add( new DynamicValue( Math.toIntExact( field.getId() ), ((RexDynamicParam) src).getIndex() ) );
                    } else if ( src instanceof RexCall && src.getType().getPolyType() == PolyType.ARRAY ) {
                        values.add( FileUtil.fromArrayRexCall( (RexCall) src ) );
                    } else {
                        throw new GenericRuntimeException( "Unknown element in sourceExpressionList: " + src.toString() );
                    }
                    i++;
                }
                implementor.setUpdates( values );
            }
            //set the columns that should be updated in the projection list
            implementor.project( this.getUpdateColumns(), null );
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
                throw new GenericRuntimeException( "The File adapter does not support " + operation + "operations." );
        }
    }

}
