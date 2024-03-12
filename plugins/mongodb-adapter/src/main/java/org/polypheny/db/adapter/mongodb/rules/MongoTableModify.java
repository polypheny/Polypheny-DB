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

package org.polypheny.db.adapter.mongodb.rules;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.gridfs.GridFSBucket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.adapter.mongodb.MongoAlg;
import org.polypheny.db.adapter.mongodb.MongoEntity;
import org.polypheny.db.adapter.mongodb.MongoPlugin.MongoStore;
import org.polypheny.db.adapter.mongodb.bson.BsonDynamic;
import org.polypheny.db.adapter.mongodb.rules.MongoRules.MongoDocuments;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.Pair;

class MongoTableModify extends RelModify<MongoEntity> implements MongoAlg {


    private final GridFSBucket bucket;
    private Implementor implementor;


    protected MongoTableModify(
            AlgCluster cluster,
            AlgTraitSet traitSet,
            MongoEntity entity,
            AlgNode input,
            Operation operation,
            List<String> updateColumns,
            List<? extends RexNode> sourceExpressions,
            boolean flattened ) {
        super( cluster, traitSet, entity, input, operation, updateColumns, sourceExpressions, flattened );
        this.bucket = entity.getMongoNamespace().getBucket();
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new MongoTableModify(
                getCluster(),
                traitSet,
                getEntity(),
                AbstractAlgNode.sole( inputs ),
                getOperation(),
                getUpdateColumns(),
                getSourceExpressions(),
                isFlattened() );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.setDML( true );
        this.implementor = implementor;

        implementor.setEntity( entity );
        implementor.setOperation( this.getOperation() );

        switch ( this.getOperation() ) {
            case INSERT:
                if ( input instanceof MongoValues ) {
                    handleDirectInsert( implementor, ((MongoValues) input) );
                } else if ( input instanceof MongoDocuments ) {
                    handleDocumentInsert( implementor, ((MongoDocuments) input) );
                } else if ( input instanceof MongoProject ) {
                    handlePreparedInsert( implementor, ((MongoProject) input) );
                } else {
                    return;
                }
                break;
            case UPDATE:
                handleUpdate( implementor );
                break;
            case MERGE:
                break;
            case DELETE:
                handleDelete( implementor );
                break;
        }
    }


    private void handleDelete( Implementor implementor ) {
        Implementor filterCollector = new Implementor( true );
        filterCollector.setEntity( entity );
        ((MongoAlg) input).implement( filterCollector );
        implementor.filter = filterCollector.filter;
        if ( Pair.right( filterCollector.list ).contains( "{$limit: 1}" ) ) {
            implementor.onlyOne = true;
        }
    }


    private void handleUpdate( Implementor implementor ) {
        Implementor condImplementor = new Implementor( true );
        condImplementor.setEntity( entity );
        ((MongoAlg) input).implement( condImplementor );
        implementor.filter = condImplementor.filter;
        //assert condImplementor.getStaticRowType() instanceof MongoRowType;
        AlgDataType rowType = condImplementor.getTupleType();
        int pos = 0;
        BsonDocument doc = new BsonDocument();
        List<BsonDocument> docDocs = new ArrayList<>();
        GridFSBucket bucket = implementor.getBucket();
        for ( RexNode el : getSourceExpressions() ) {
            String physicalName = entity.getPhysicalName( getUpdateColumns().get( pos ) );
            if ( el.isA( Kind.LITERAL ) ) {
                doc.append(
                        physicalName,//getPhysicalName( getUpdateColumns().get( pos ), implementor ),
                        BsonUtil.getAsBson( (RexLiteral) el, bucket ) );
            } else if ( el instanceof RexCall call ) {
                if ( Arrays.asList( Kind.PLUS, Kind.PLUS, Kind.TIMES, Kind.DIVIDE ).contains( call.op.getKind() ) ) {
                    doc.append(
                            physicalName,//rowType.getPhysicalName( getUpdateColumns().get( pos ), implementor ),
                            visitCall( implementor, (RexCall) el, call.op.getKind(), el.getType().getPolyType() ) );
                } else if ( call.op.getKind().belongsTo( Kind.MQL_KIND ) ) {
                    docDocs.add( handleDocumentUpdate( (RexCall) el, bucket, rowType ) );
                } else {
                    doc.append(
                            physicalName,//rowType.getPhysicalName( getUpdateColumns().get( pos ), implementor ),
                            BsonUtil.getBsonArray( call, bucket ) );
                }
            } else if ( el.isA( Kind.DYNAMIC_PARAM ) ) {
                doc.append(
                        physicalName,//rowType.getPhysicalName( getUpdateColumns().get( pos ), implementor ),
                        new BsonDynamic( (RexDynamicParam) el ) );
            } else if ( el.isA( Kind.FIELD_ACCESS ) ) {
                doc.append(
                        physicalName,//rowType.getPhysicalName( getUpdateColumns().get( pos ), implementor ),
                        new BsonString(
                                "$" + rowType.getFields().get( el.unwrap( RexFieldAccess.class ).orElseThrow().getField().getIndex() ).getPhysicalName() ) );//rowType.getPhysicalName( ((RexFieldAccess) el).getField().getName(), implementor ) ) );
            }
            pos++;
        }
        if ( !doc.isEmpty() ) {
            BsonDocument update = new BsonDocument().append( "$set", doc );

            implementor.operations = Collections.singletonList( update );
        } else {
            implementor.operations = docDocs;
        }

        if ( Pair.right( condImplementor.list ).contains( "{$limit: 1}" ) ) {
            implementor.onlyOne = true;
        }
    }


    private BsonDocument handleDocumentUpdate( RexCall el, GridFSBucket bucket, AlgDataType rowType ) {
        if ( el.op.getKind() == Kind.MQL_JSONIFY ) {
            assert el.getOperands().size() == 1;
            return handleDocumentUpdate( (RexCall) el.getOperands().get( 0 ), bucket, rowType );
        }

        BsonDocument doc = new BsonDocument();
        assert el.getOperands().size() >= 2;
        assert el.getOperands().get( 0 ) instanceof RexIndexRef;

        String key = getDocParentKey( (RexIndexRef) el.operands.get( 0 ), rowType );
        attachUpdateStep( doc, el, rowType, key );

        return doc;
    }


    private void attachUpdateStep( BsonDocument doc, RexCall el, AlgDataType rowType, String key ) {
        List<String> keys = getDocUpdateKey( (RexIndexRef) el.operands.get( 0 ), (RexCall) el.operands.get( 1 ), rowType );
        switch ( el.op.getKind() ) {
            case MQL_UPDATE_REPLACE:
                assert el.getOperands().size() == 3;
                assert el.getOperands().get( 2 ) instanceof RexCall;

                doc.putAll( getReplaceUpdate( keys, (RexCall) el.operands.get( 2 ), implementor, bucket ) );
                break;
            case MQL_ADD_FIELDS:
                assert el.getOperands().size() == 3;
                assert el.getOperands().get( 2 ) instanceof RexCall;

                doc.putAll( getAddUpdate( keys, (RexCall) el.operands.get( 2 ) ) );
                break;
            case MQL_UPDATE_REMOVE:
                assert el.getOperands().size() == 2;

                doc.putAll( getRemoveUpdate( keys, (RexCall) el.operands.get( 1 ) ) );
                break;
            case MQL_UPDATE_RENAME:
                assert el.getOperands().size() == 3;
                assert el.getOperands().get( 2 ) instanceof RexCall;

                doc.putAll( getRenameUpdate( keys, key, (RexCall) el.operands.get( 2 ) ) );
                break;
            default:
                throw new RuntimeException( "The used update operation is not supported by the MongoDB adapter." );
        }
    }


    private String getDocParentKey( RexIndexRef rexInputRef, AlgDataType rowType ) {
        return rowType.getFieldNames().get( rexInputRef.getIndex() );
    }


    private BsonDocument getRenameUpdate( List<String> keys, String parentKey, RexCall call ) {
        BsonDocument doc = new BsonDocument();
        assert keys.size() == call.operands.size();
        int pos = 0;
        for ( String key : keys ) {
            doc.put( key, new BsonString( parentKey + "." + ((RexLiteral) call.operands.get( pos )).getValue() ) );
            pos++;
        }

        return new BsonDocument( "$rename", doc );
    }


    private BsonDocument getRemoveUpdate( List<String> keys, RexCall call ) {
        BsonDocument doc = new BsonDocument();
        for ( String key : keys ) {
            doc.put( key, new BsonString( "" ) );
        }

        return new BsonDocument( "$unset", doc );
    }


    private BsonDocument getAddUpdate( List<String> keys, RexCall call ) {
        BsonDocument doc = new BsonDocument();
        assert keys.size() == call.operands.size();
        int pos = 0;
        for ( String key : keys ) {
            doc.put( key, BsonUtil.getAsBson( (RexLiteral) call.operands.get( pos ), this.bucket ) );
            pos++;
        }

        return new BsonDocument( "$set", doc );
    }


    public static BsonDocument getReplaceUpdate( List<String> keys, RexCall call, Implementor implementor, GridFSBucket bucket ) {
        BsonDocument doc = new BsonDocument();
        assert keys.size() == call.operands.size();

        int pos = 0;
        for ( RexNode operand : call.operands ) {
            if ( !(operand instanceof RexCall op) ) {
                doc.append( "$set", new BsonDocument( keys.get( pos ), BsonUtil.getAsBson( (RexLiteral) operand, bucket ) ) );
            } else {
                implementor.isDocumentUpdate = true;
                switch ( op.getKind() ) {
                    case PLUS:
                        doc.append( "$inc", new BsonDocument( keys.get( pos ), BsonUtil.getAsBson( (RexLiteral) op.operands.get( 1 ), bucket ) ) );
                        break;
                    case TIMES:
                        doc.append( "$mul", new BsonDocument( keys.get( pos ), BsonUtil.getAsBson( (RexLiteral) op.operands.get( 1 ), bucket ) ) );
                        break;
                    case MIN:
                        doc.append( "$min", new BsonDocument( keys.get( pos ), BsonUtil.getAsBson( (RexLiteral) op.operands.get( 1 ), bucket ) ) );
                        break;
                    case MAX:
                        doc.append( "$max", new BsonDocument( keys.get( pos ), BsonUtil.getAsBson( (RexLiteral) op.operands.get( 1 ), bucket ) ) );
                        break;
                }
            }
            pos++;
        }

        return doc;
    }


    private List<String> getDocUpdateKey( RexIndexRef row, RexCall subfield, AlgDataType rowType ) {
        String name = rowType.getFieldNames().get( row.getIndex() );
        return subfield
                .operands
                .stream()
                .map( n -> ((RexLiteral) n).getValue() )
                .map( n -> name + "." + n )
                .toList();
    }


    private void handleDocumentInsert( Implementor implementor, MongoDocuments documents ) {
        implementor.operations = documents.documents
                .stream()
                .filter( PolyValue::isDocument )
                .map( d -> BsonDocument.parse( d.toTypedJson() ) )
                .toList();
    }


    private BsonValue visitCall( Implementor implementor, RexCall call, Kind op, PolyType type ) {
        BsonDocument doc = new BsonDocument();

        BsonArray array = new BsonArray();
        for ( RexNode operand : call.operands ) {
            if ( operand.getKind() == Kind.FIELD_ACCESS ) {
                String physicalName = "$" + implementor.getPhysicalName( ((RexFieldAccess) operand).getField().getName() );
                array.add( new BsonString( physicalName ) );
            } else if ( operand instanceof RexCall ) {
                array.add( visitCall( implementor, (RexCall) operand, ((RexCall) operand).op.getKind(), type ) );
            } else if ( operand.getKind() == Kind.LITERAL ) {
                array.add( BsonUtil.getAsBson( ((RexLiteral) operand).getValue(), type, bucket ) );
            } else if ( operand.getKind() == Kind.DYNAMIC_PARAM ) {
                array.add( new BsonDynamic( (RexDynamicParam) operand ) );
            } else {
                throw new RuntimeException( "Not implemented yet" );
            }
        }
        switch ( op ) {
            case PLUS:
                doc.append( "$add", array );
                break;
            case MINUS:
                doc.append( "$subtract", array );
                break;
            case TIMES:
                doc.append( "$multiply", array );
                break;
            case DIVIDE:
                doc.append( "$divide", array );
                break;
            default:
                throw new RuntimeException( "Not implemented yet" );
        }

        return doc;
    }


    private void handlePreparedInsert( Implementor implementor, MongoProject input ) {
        if ( !(input.getInput() instanceof MongoValues || input.getInput() instanceof MongoDocuments) && input.getInput().getTupleType().getFields().size() == 1 ) {
            return;
        }

        BsonDocument doc = new BsonDocument();
        MongoEntity entity = implementor.getEntity();
        GridFSBucket bucket = implementor.getBucket();
        //noinspection AssertWithSideEffects
        assert input.getTupleType().getFieldCount() == this.getEntity().getTupleType().getFieldCount();
        implementor.setEntity( entity );

        int pos = 0;
        for ( RexNode rexNode : input.getChildExps() ) {
            String physicalName = entity.getPhysicalName( input.getTupleType().getFields().get( pos ).getName() );
            if ( rexNode instanceof RexDynamicParam ) {
                // preparedInsert
                doc.append( physicalName, new BsonDynamic( (RexDynamicParam) rexNode ) );
            } else if ( rexNode instanceof RexLiteral ) {
                doc.append( physicalName, BsonUtil.getAsBson( (RexLiteral) rexNode, bucket ) );
            } else if ( rexNode instanceof RexCall ) {
                PolyType type = this.entity
                        .getTupleType( getCluster().getTypeFactory() )
                        .getFields()
                        .get( pos )
                        .getType()
                        .getComponentType()
                        .getPolyType();

                doc.append( physicalName, getBsonArray( (RexCall) rexNode, type, bucket ) );

            } else if ( rexNode.getKind() == Kind.INPUT_REF && input.getInput() instanceof MongoValues ) {
                handleDirectInsert( implementor, (MongoValues) input.getInput() );
                return;
            } else {
                throw new GenericRuntimeException( "This rexType was not considered" );
            }

            pos++;
        }
        implementor.operations = Collections.singletonList( doc );
    }


    private Map<Integer, String> getPhysicalMap( List<AlgDataTypeField> fieldList, PhysicalCollection catalogCollection ) {
        Map<Integer, String> map = new HashMap<>();
        map.put( 0, "d" );
        return map;
    }


    private BsonValue getBsonArray( RexCall el, PolyType type, GridFSBucket bucket ) {
        if ( el.op.getKind() == Kind.ARRAY_VALUE_CONSTRUCTOR ) {
            BsonArray array = new BsonArray();
            array.addAll( el.operands.stream().map( operand -> {
                if ( operand instanceof RexLiteral ) {
                    return BsonUtil.getAsBson( ((RexLiteral) operand).value, type, bucket );
                } else if ( operand instanceof RexCall ) {
                    return getBsonArray( (RexCall) operand, type, bucket );
                }
                throw new RuntimeException( "The given RexCall could not be transformed correctly." );
            } ).toList() );
            return array;
        }
        throw new RuntimeException( "The given RexCall could not be transformed correctly." );
    }


    private void handleDirectInsert( Implementor implementor, MongoValues values ) {
        List<BsonDocument> docs = new ArrayList<>();
        MongoEntity entity = implementor.getEntity();
        GridFSBucket bucket = implementor.bucket;

        AlgDataType valRowType = rowType;

        if ( valRowType == null ) {
            valRowType = values.getTupleType();
        }

        List<String> columnNames = entity.getTupleType().getFieldNames();
        List<Long> columnIds = entity.getTupleType().getFieldIds();
        for ( ImmutableList<RexLiteral> literals : values.tuples ) {
            BsonDocument doc = new BsonDocument();
            int pos = 0;
            for ( RexLiteral literal : literals ) {
                String name = valRowType.getFieldNames().get( pos );
                if ( columnNames.contains( name ) ) {
                    doc.append(
                            MongoStore.getPhysicalColumnName( columnIds.get( columnNames.indexOf( name ) ) ),
                            BsonUtil.getAsBson( literal, bucket ) );
                } else {
                    doc.append(
                            valRowType.getFieldNames().get( pos ),
                            BsonUtil.getAsBson( literal, bucket ) );
                }
                pos++;
            }
            docs.add( doc );
        }
        implementor.operations = docs;
    }

}
