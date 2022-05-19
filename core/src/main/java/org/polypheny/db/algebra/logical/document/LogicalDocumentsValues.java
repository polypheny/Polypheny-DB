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

package org.polypheny.db.algebra.logical.document;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.document.DocumentsValues;
import org.polypheny.db.algebra.core.relational.RelationalTransformable;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.BsonUtil;


public class LogicalDocumentsValues extends DocumentsValues implements RelationalTransformable {

    private final static PolyTypeFactoryImpl typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );


    /**
     * Java representation of multiple documents, which can be retrieved in the original BSON format form
     * or in the substantiated relational form, where the documents are bundled into a BSON string
     *
     * BSON format
     * <pre><code>
     *     "_id": ObjectId(23kdf232123)
     *     "key": "value",
     *     "key1": "value"
     * </pre></code>
     *
     * becomes
     *
     * Column format
     * <pre><code>
     *     "_id": ObjectId(23kdf232123)
     *     "_data": {
     *         "key": "value",
     *         "key1": "value"
     *     }
     *
     * </pre></code>
     *
     * @param cluster the cluster, which holds the information regarding the ongoing operation
     * @param defaultRowType, substitution rowType, which is "_id", "_data" and possible fixed columns if they exist
     * @param traitSet the used traitSet
     * @param tuples the documents in their native BSON format
     */
    public LogicalDocumentsValues( AlgOptCluster cluster, AlgDataType defaultRowType, AlgTraitSet traitSet, ImmutableList<BsonValue> tuples ) {
        super( cluster, traitSet, defaultRowType, tuples );
    }


    public static AlgNode create( AlgOptCluster cluster, ImmutableList<BsonValue> values ) {
        List<AlgDataTypeField> fields = new ArrayList<>();
        fields.add( new AlgDataTypeFieldImpl( "_id", 0, typeFactory.createPolyType( PolyType.VARCHAR, 24 ) ) );
        fields.add( new AlgDataTypeFieldImpl( "_data", 1, typeFactory.createPolyType( PolyType.JSON ) ) );//typeFactory.createMapType( typeFactory.createPolyType( PolyType.VARCHAR, 2024 ), typeFactory.createPolyType( PolyType.ANY ) ) ) );
        AlgDataType defaultRowType = new AlgRecordType( fields );

        return create( cluster, getOrAddId( values ), defaultRowType );
    }


    public static AlgNode create( AlgOptCluster cluster, ImmutableList<BsonValue> tuples, AlgDataType defaultRowType ) {
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalDocumentsValues( cluster, defaultRowType, traitSet, tuples );
    }


    public static AlgNode create( LogicalValues input ) {
        return create( input.getCluster(), bsonify( input.getTuples(), input.getRowType() ), input.getRowType() );
    }


    private static ImmutableList<BsonValue> bsonify( ImmutableList<ImmutableList<RexLiteral>> tuples, AlgDataType rowType ) {
        List<BsonValue> docs = new ArrayList<>();

        for ( ImmutableList<RexLiteral> values : tuples ) {
            BsonDocument doc = new BsonDocument();
            int pos = 0;
            for ( RexLiteral value : values ) {
                AlgDataTypeField field = rowType.getFieldList().get( pos );

                if ( field.getName().equals( "_id" ) ) {
                    String _id = value.getValueAs( String.class );
                    ObjectId objectId;
                    if ( _id.matches( "ObjectId\\([0-9abcdef]{24}\\)" ) ) {
                        objectId = new ObjectId( _id.substring( 9, 33 ) );
                    } else {
                        objectId = ObjectId.get();
                    }
                    doc.put( "_id", new BsonObjectId( objectId ) );
                } else if ( field.getName().equals( "_data" ) ) {
                    BsonDocument docVal = new BsonDocument();
                    if ( !value.isNull() && value.getValueAs( String.class ).length() != 0 ) {
                        String data = BsonUtil.fixBson( value.getValueAs( String.class ) );
                        if ( data.matches( "[{].*[}]" ) ) {
                            docVal = BsonDocument.parse( data );
                        } else {
                            throw new RuntimeException( "The inserted document is not valid." );
                        }
                    }
                    doc.put( "_data", docVal );
                } else {
                    doc.put( field.getName(), BsonUtil.getAsBson( value, null ) );
                }

                pos++;
            }
            docs.add( doc );
        }
        return ImmutableList.copyOf( docs );
    }


    @Override
    public NamespaceType getModel() {
        return NamespaceType.DOCUMENT;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        assert inputs.isEmpty();
        return new LogicalDocumentsValues( getCluster(), rowType, traitSet, documentTuples );
    }


    private static ImmutableList<BsonValue> getOrAddId( ImmutableList<BsonValue> values ) {
        List<BsonValue> docs = new ArrayList<>();

        for ( BsonValue value : values ) {
            if ( value.isDocument() ) {
                BsonValue id = new BsonString( new BsonObjectId().getValue().toString() );
                if ( value.asDocument().containsKey( "_id" ) ) {
                    id = value.asDocument().get( "_id" );
                }
                value.asDocument().put( "_id", id );
            }
            docs.add( value );
        }
        return ImmutableList.copyOf( docs );
    }


    private static ImmutableList<ImmutableList<RexLiteral>> normalizeToMap( List<BsonValue> tuples, AlgDataType rowType, RexBuilder rexBuilder ) {
        List<ImmutableList<RexLiteral>> normalized = new ArrayList<>();

        for ( BsonValue tuple : tuples ) {
            List<RexLiteral> normalizedTuple = new ArrayList<>();
            normalizedTuple.add( 0, rexBuilder.makeLiteral( ObjectId.get().toString() ) );

            Map<String, BsonValue> parsed = tuple.asDocument();
            normalizedTuple.add( 1, rexBuilder.makeMapFromBson( rowType.getFieldList().get( 1 ).getType(), parsed ) );
            normalized.add( ImmutableList.copyOf( normalizedTuple ) );
        }

        return ImmutableList.copyOf( normalized );

    }


    @Override
    public List<AlgNode> getRelationalEquivalent( List<AlgNode> values, List<AlgOptTable> entities ) {
        AlgTraitSet out = traitSet.replace( ModelTrait.RELATIONAL );

        AlgOptCluster cluster = AlgOptCluster.create( getCluster().getPlanner(), getCluster().getRexBuilder() );

        LogicalValues jsonValues = new LogicalValues( cluster, out, entities.get( 0 ).getRowType(), normalize( documentTuples, cluster.getRexBuilder() ) );

        return List.of( jsonValues );
    }


}
