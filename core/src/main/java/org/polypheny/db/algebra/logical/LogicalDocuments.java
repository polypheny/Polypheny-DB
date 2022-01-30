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

package org.polypheny.db.algebra.logical;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Documents;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.BsonUtil;


public class LogicalDocuments extends LogicalValues implements Documents {

    private final static PolyTypeFactoryImpl typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );

    private final AlgDataType rowType;
    @Getter
    private final ImmutableList<BsonValue> documentTuples;


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
     * @param normalizedTuples the documents in their substituted relational format
     */
    public LogicalDocuments( AlgOptCluster cluster, AlgDataType defaultRowType, AlgTraitSet traitSet, ImmutableList<BsonValue> tuples, ImmutableList<ImmutableList<RexLiteral>> normalizedTuples ) {
        super( cluster, traitSet, defaultRowType, validateLiterals( normalizedTuples, defaultRowType, cluster.getRexBuilder() ) );
        this.documentTuples = validate( tuples, defaultRowType );
        this.rowType = defaultRowType;
    }


    public static AlgNode create( AlgOptCluster cluster, ImmutableList<BsonValue> values ) {
        List<AlgDataTypeField> fields = new ArrayList<>();
        fields.add( new AlgDataTypeFieldImpl( "_id", 0, typeFactory.createPolyType( PolyType.VARCHAR, 24 ) ) );
        fields.add( new AlgDataTypeFieldImpl( "_data", 1, typeFactory.createMapType( typeFactory.createPolyType( PolyType.ANY ), typeFactory.createPolyType( PolyType.ANY ) ) ) );
        AlgDataType defaultRowType = new AlgRecordType( fields );

        //ImmutableList<ImmutableList<RexLiteral>> normalizedTuples = normalize( tuples, rowTypes, defaultRowType );

        return create( cluster, getOrAddId( values ), defaultRowType, normalizeToMap( values, defaultRowType, cluster.getRexBuilder() ) );
    }


    public static AlgNode create( AlgOptCluster cluster, ImmutableList<BsonValue> tuples, AlgDataType defaultRowType, ImmutableList<ImmutableList<RexLiteral>> normalizedTuples ) {
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE )
                .replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.values( mq, defaultRowType, normalizedTuples ) );
        return new LogicalDocuments( cluster, defaultRowType, traitSet, tuples, normalizedTuples );
    }


    public static AlgNode create( LogicalValues input ) {
        return create( input.getCluster(), bsonify( input.getTuples(), input.getRowType() ), input.getRowType(), input.getTuples() );
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
    public SchemaType getModel() {
        return SchemaType.DOCUMENT;
    }


    @Override
    public ImmutableList<ImmutableList<RexLiteral>> getTuples( AlgInput input ) {
        return super.getTuples( input );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        assert inputs.isEmpty();
        return new LogicalDocuments( getCluster(), rowType, traitSet, documentTuples, tuples );
    }


    private static ImmutableList<BsonValue> getOrAddId( ImmutableList<BsonValue> values ) {
        List<BsonValue> docs = new ArrayList<>();

        for ( BsonValue value : values ) {
            BsonDocument doc = new BsonDocument();
            if ( value.isDocument() ) {
                BsonValue id;
                if ( value.asDocument().containsKey( "_id" ) ) {
                    id = value.asDocument().get( "_id" );
                } else {
                    id = new BsonObjectId();
                }
                doc.put( "_id", id );

                value.asDocument().remove( "_id" );
                doc.put( "_data", value );
            }
            docs.add( doc );
        }
        return ImmutableList.copyOf( docs );
    }


    private ImmutableList<BsonValue> validate( ImmutableList<BsonValue> tuples, AlgDataType defaultRowType ) {
        List<BsonValue> docs = new ArrayList<>();
        List<String> names = defaultRowType.getFieldNames();

        for ( BsonValue tuple : tuples ) {
            BsonDocument document = new BsonDocument();
            if ( tuple.isDocument() ) {
                for ( String name : names ) {
                    if ( tuple.asDocument().containsKey( name ) ) {
                        document.put( name, tuple.asDocument().get( name ) );
                    } else {
                        document.put( name, new BsonNull() );
                    }
                }
                BsonDocument data = new BsonDocument();
                if ( tuple.asDocument().containsKey( "_data" ) ) {
                    data.putAll( tuple.asDocument().get( "_data" ).asDocument() );
                }
                tuple.asDocument()
                        .entrySet()
                        .stream()
                        .filter( e -> !names.contains( e.getKey() ) )
                        .forEach( k -> data.put( k.getKey(), k.getValue() ) );

            }
            docs.add( document );
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


    private static ImmutableList<ImmutableList<RexLiteral>> normalize( List<BsonValue> tuples, RexBuilder rexBuilder ) {
        List<ImmutableList<RexLiteral>> normalized = new ArrayList<>();

        JsonWriterSettings writerSettings = JsonWriterSettings.builder().outputMode( JsonMode.STRICT ).build();

        for ( BsonValue tuple : tuples ) {
            List<RexLiteral> normalizedTuple = new ArrayList<>();
            normalizedTuple.add( 0, rexBuilder.makeLiteral( ObjectId.get().toString() ) );
            //normalizedTuple.add( 0, new RexLiteral( new NlsString( ObjectId.get().toString(), "ISO-8859-1", SqlCollation.IMPLICIT ), typeFactory.createPolyType( PolyType.CHAR, 24 ), PolyType.CHAR ) );
            String parsed = tuple.asDocument().toJson( writerSettings );
            normalizedTuple.add( 1, rexBuilder.makeLiteral( parsed ) );
            //normalizedTuple.add( new RexLiteral( new NlsString( parsed, "ISO-8859-1", SqlCollation.IMPLICIT ), typeFactory.createPolyType( PolyType.CHAR, parsed.length() ), PolyType.CHAR ) );
            normalized.add( ImmutableList.copyOf( normalizedTuple ) );
        }

        return ImmutableList.copyOf( normalized );
    }


    private static ImmutableList<ImmutableList<RexLiteral>> validateLiterals( ImmutableList<ImmutableList<RexLiteral>> tuples, AlgDataType rowType, RexBuilder rexBuilder ) {
        List<ImmutableList<RexLiteral>> validated = new ArrayList<>();
        List<String> names = rowType.getFieldNames();
        for ( ImmutableList<RexLiteral> values : tuples ) {
            List<RexLiteral> row = new ArrayList<>();
            int pos = 0;
            for ( String name : names ) {
                if ( name.equals( "_id" ) ) {
                    String id = values.get( pos ).getValueAs( String.class );
                    if ( id.matches( "ObjectId\\([0-9abcdef]{24}\\)" ) ) {
                        id = id.substring( 9, 33 );
                    } else {
                        id = ObjectId.get().toString();
                    }
                    row.add( rexBuilder.makeLiteral( id ) );

                } else {
                    row.add( values.get( pos ) );
                }
                pos++;
            }
            validated.add( ImmutableList.copyOf( row ) );
        }

        return ImmutableList.copyOf( validated );
    }

}
