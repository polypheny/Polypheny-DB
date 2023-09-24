/*
 * Copyright 2019-2023 The Polypheny Project
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
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.adapter.mongodb.MongoAlg;
import org.polypheny.db.adapter.mongodb.MongoEntity;
import org.polypheny.db.adapter.mongodb.MongoPlugin.MongoStore;
import org.polypheny.db.adapter.mongodb.MongoRowType;
import org.polypheny.db.adapter.mongodb.bson.BsonDynamic;
import org.polypheny.db.adapter.mongodb.rules.MongoRules.MongoDocuments;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
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
            AlgOptCluster cluster,
            AlgTraitSet traitSet,
            MongoEntity entity,
            AlgNode input,
            Operation operation,
            List<String> updateColumnList,
            List<? extends RexNode> sourceExpressionList,
            boolean flattened ) {
        super( cluster, traitSet, entity, input, operation, updateColumnList, sourceExpressionList, flattened );
        this.bucket = entity.unwrap( MongoEntity.class ).getMongoNamespace().getBucket();
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
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
                getUpdateColumnList(),
                getSourceExpressionList(),
                isFlattened() );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.setDML( true );
        this.implementor = implementor;

        implementor.entity = entity;
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
        filterCollector.setStaticRowType( implementor.getStaticRowType() );
        ((MongoAlg) input).implement( filterCollector );
        implementor.filter = filterCollector.filter;
        if ( Pair.right( filterCollector.list ).contains( "{$limit: 1}" ) ) {
            implementor.onlyOne = true;
        }
    }


    private void handleUpdate( Implementor implementor ) {
        Implementor condImplementor = new Implementor( true );
        condImplementor.setStaticRowType( implementor.getStaticRowType() );
        ((MongoAlg) input).implement( condImplementor );
        implementor.filter = condImplementor.filter;
        assert condImplementor.getStaticRowType() instanceof MongoRowType;
        MongoRowType rowType = (MongoRowType) condImplementor.getStaticRowType();
        int pos = 0;
        BsonDocument doc = new BsonDocument();
        List<BsonDocument> docDocs = new ArrayList<>();
        GridFSBucket bucket = implementor.getBucket();
        for ( RexNode el : getSourceExpressionList() ) {
            if ( el.isA( Kind.LITERAL ) ) {
                doc.append(
                        rowType.getPhysicalName( getUpdateColumnList().get( pos ), implementor ),
                        BsonUtil.getAsBson( (RexLiteral) el, bucket ) );
            } else if ( el instanceof RexCall ) {
                RexCall call = ((RexCall) el);
                if ( Arrays.asList( Kind.PLUS, Kind.PLUS, Kind.TIMES, Kind.DIVIDE ).contains( call.op.getKind() ) ) {
                    doc.append(
                            rowType.getPhysicalName( getUpdateColumnList().get( pos ), implementor ),
                            visitCall( implementor, (RexCall) el, call.op.getKind(), el.getType().getPolyType() ) );
                } else if ( call.op.getKind().belongsTo( Kind.MQL_KIND ) ) {
                    docDocs.add( handleDocumentUpdate( (RexCall) el, bucket, rowType ) );
                } else {
                    doc.append(
                            rowType.getPhysicalName( getUpdateColumnList().get( pos ), implementor ),
                            BsonUtil.getBsonArray( call, bucket ) );
                }
            } else if ( el.isA( Kind.DYNAMIC_PARAM ) ) {
                doc.append(
                        rowType.getPhysicalName( getUpdateColumnList().get( pos ), implementor ),
                        new BsonDynamic( (RexDynamicParam) el ) );
            } else if ( el.isA( Kind.FIELD_ACCESS ) ) {
                doc.append(
                        rowType.getPhysicalName( getUpdateColumnList().get( pos ), implementor ),
                        new BsonString(
                                "$" + rowType.getPhysicalName(
                                        ((RexFieldAccess) el).getField().getName(), implementor ) ) );
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


    private BsonDocument handleDocumentUpdate( RexCall el, GridFSBucket bucket, MongoRowType rowType ) {
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


    private void attachUpdateStep( BsonDocument doc, RexCall el, MongoRowType rowType, String key ) {
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


    private String getDocParentKey( RexIndexRef rexInputRef, MongoRowType rowType ) {
        return rowType.getFieldNames().get( rexInputRef.getIndex() );
    }


    private BsonDocument getRenameUpdate( List<String> keys, String parentKey, RexCall call ) {
        BsonDocument doc = new BsonDocument();
        assert keys.size() == call.operands.size();
        int pos = 0;
        for ( String key : keys ) {
            doc.put( key, new BsonString( parentKey + "." + ((RexLiteral) call.operands.get( pos )).getValueAs( String.class ) ) );
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
            if ( !(operand instanceof RexCall) ) {
                doc.append( "$set", new BsonDocument( keys.get( pos ), BsonUtil.getAsBson( (RexLiteral) operand, bucket ) ) );
            } else {
                RexCall op = (RexCall) operand;
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


    private List<String> getDocUpdateKey( RexIndexRef row, RexCall subfield, MongoRowType rowType ) {
        String name = rowType.getFieldNames().get( row.getIndex() );
        return subfield
                .operands
                .stream()
                .map( n -> ((RexLiteral) n).getValueAs( String.class ) )
                .map( n -> name + "." + n )
                .collect( Collectors.toList() );
    }


    private void handleDocumentInsert( Implementor implementor, MongoDocuments documents ) {
        implementor.operations = documents.documents
                .stream()
                .filter( PolyValue::isDocument )
                .map( d -> BsonDocument.parse( d.toJson() ) )
                .collect( Collectors.toList() );
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
                array.add( BsonUtil.getAsBson( ((RexLiteral) operand).getValueAs( BsonUtil.getClassFromType( type ) ), type, bucket ) );
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
        if ( !(input.getInput() instanceof MongoValues || input.getInput() instanceof MongoDocuments) && input.getInput().getRowType().getFieldList().size() == 1 ) {
            return;
        }

        BsonDocument doc = new BsonDocument();
        MongoEntity entity = implementor.entity.unwrap( MongoEntity.class );
        GridFSBucket bucket = implementor.getBucket();
        //noinspection AssertWithSideEffects
        assert input.getRowType().getFieldCount() == this.getEntity().getRowType().getFieldCount();
        /*Map<Integer, String> physicalMapping;
        if ( input.getInput() instanceof MongoValues ) {
            physicalMapping = getPhysicalMap( input.getRowType().getFieldList(), table );
        } else if ( input.getInput() instanceof MongoDocuments ) {
            physicalMapping = getPhysicalMap( input.getRowType().getFieldList(), implementor.entity.unwrap( PhysicalCollection.class ) );
        } else {
            throw new GenericRuntimeException( "Mapping for physical mongo fields not found" );
        }*/

        implementor.setStaticRowType( (AlgRecordType) input.getRowType() );

        int pos = 0;
        for ( RexNode rexNode : input.getChildExps() ) {
            if ( rexNode instanceof RexDynamicParam ) {
                // preparedInsert
                doc.append( entity.fields.get( pos ).name, new BsonDynamic( (RexDynamicParam) rexNode ) );
            } else if ( rexNode instanceof RexLiteral ) {
                doc.append( entity.fields.get( pos ).name, BsonUtil.getAsBson( (RexLiteral) rexNode, bucket ) );
            } else if ( rexNode instanceof RexCall ) {
                PolyType type = this.entity
                        .getRowType( getCluster().getTypeFactory() )
                        .getFieldList()
                        .get( pos )
                        .getType()
                        .getComponentType()
                        .getPolyType();

                doc.append( entity.fields.get( pos ).name, getBsonArray( (RexCall) rexNode, type, bucket ) );

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


    private Map<Integer, String> getPhysicalMap( List<AlgDataTypeField> fieldList, LogicalTable table ) {
        Map<Integer, String> map = new HashMap<>();
        List<String> names = table.getColumnNames();
        List<Long> ids = table.getColumnIds();
        int pos = 0;
        for ( String name : Pair.left( fieldList ) ) {
            map.put( pos, MongoStore.getPhysicalColumnName( ids.get( names.indexOf( name ) ) ) );
            pos++;
        }
        return map;
    }


    private String getPhysicalName( MongoProject input, LogicalTable table, int pos ) {
        String logicalName = input.getRowType().getFieldNames().get( pos );
        int index = table.getColumnNames().indexOf( logicalName );
        return MongoStore.getPhysicalColumnName( table.getColumnIds().get( index ) );
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
            } ).collect( Collectors.toList() ) );
            return array;
        }
        throw new RuntimeException( "The given RexCall could not be transformed correctly." );
    }


    private void handleDirectInsert( Implementor implementor, MongoValues values ) {
        List<BsonDocument> docs = new ArrayList<>();
        LogicalTable catalogTable = implementor.entity.unwrap( LogicalTable.class );
        GridFSBucket bucket = implementor.bucket;

        AlgDataType valRowType = rowType;

        if ( valRowType == null ) {
            valRowType = values.getRowType();
        }

        List<String> columnNames = catalogTable.getColumnNames();
        List<Long> columnIds = catalogTable.getColumnIds();
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
                            rowType.getFieldNames().get( pos ),
                            BsonUtil.getAsBson( literal, bucket ) );
                }
                pos++;
            }
            docs.add( doc );
        }
        implementor.operations = docs;
    }

}
