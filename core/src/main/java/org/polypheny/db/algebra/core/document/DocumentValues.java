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

package org.polypheny.db.algebra.core.document;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.type.PolyType;


public abstract class DocumentValues extends AbstractAlgNode implements DocumentAlg {

    @Getter
    public final ImmutableList<BsonValue> documentTuples;


    /**
     * Creates a {@link DocumentValues}.
     * {@link org.polypheny.db.schema.ModelTrait#DOCUMENT} node, which contains values.
     */
    public DocumentValues( AlgOptCluster cluster, AlgTraitSet traitSet, AlgDataType rowType, ImmutableList<BsonValue> documentTuples ) {
        super( cluster, traitSet );
        this.rowType = rowType;
        this.documentTuples = validate( documentTuples );
    }


    protected static ImmutableList<BsonValue> validate( ImmutableList<BsonValue> tuples ) {
        List<BsonValue> docs = new ArrayList<>();

        for ( BsonValue tuple : tuples ) {
            BsonDocument doc = new BsonDocument();
            doc.putAll( tuple.asDocument() );

            String id = "_id";
            if ( !doc.containsKey( id ) ) {
                doc.put( id, new BsonString( ObjectId.get().toString() ) );
            } else {
                if ( doc.get( id ).isObjectId() ) {
                    doc.replace( id, new BsonString( doc.get( id ).asObjectId().toString() ) );
                }
                if ( doc.get( id ).isString() && doc.get( id ).asString().getValue().length() > 24 ) {
                    throw new RuntimeException( "ObjectId was malformed." );
                }
            }
            docs.add( doc );
        }
        return ImmutableList.copyOf( docs );
    }


    protected static ImmutableList<ImmutableList<RexLiteral>> relationalize( List<BsonValue> tuples, RexBuilder rexBuilder ) {
        List<ImmutableList<RexLiteral>> normalized = new ArrayList<>();

        JsonWriterSettings writerSettings = JsonWriterSettings.builder().outputMode( JsonMode.STRICT ).build();

        for ( BsonValue tuple : tuples ) {
            List<RexLiteral> normalizedTuple = new ArrayList<>();
            String id = ObjectId.get().toString();
            if ( tuple.isDocument() && tuple.asDocument().containsKey( "_id" ) ) {
                BsonValue bsonId = tuple.asDocument().get( "_id" );
                if ( bsonId.isObjectId() ) {
                    id = bsonId.asObjectId().toString();
                } else if ( bsonId.isString() ) {
                    id = bsonId.asString().getValue();
                } else {
                    throw new RuntimeException( "Error while transforming document to relational values" );
                }

            }

            normalizedTuple.add( 0, rexBuilder.makeLiteral( id ) );
            String parsed = tuple.asDocument().toJson( writerSettings );
            normalizedTuple.add( 1, rexBuilder.makeLiteral( parsed ) );
            normalized.add( ImmutableList.copyOf( normalizedTuple ) );
        }

        return ImmutableList.copyOf( normalized );
    }


    @Override
    public String algCompareString() {
        return getClass().getCanonicalName() + "$" + documentTuples.hashCode() + "$";
    }


    @Override
    public DocType getDocType() {
        return DocType.VALUES;
    }


    public LogicalValues getRelationalEquivalent() {
        AlgTraitSet out = traitSet.replace( ModelTrait.RELATIONAL );
        AlgOptCluster cluster = AlgOptCluster.create( getCluster().getPlanner(), getCluster().getRexBuilder() );

        AlgRecordType rowType = new AlgRecordType( List.of(
                new AlgDataTypeFieldImpl( "_id_", 0, cluster.getTypeFactory().createPolyType( PolyType.VARCHAR, 24 ) ),
                new AlgDataTypeFieldImpl( "_data_", 1, cluster.getTypeFactory().createPolyType( PolyType.VARCHAR, 2024 ) ) ) );

        return new LogicalValues( cluster, out, rowType, relationalize( documentTuples, cluster.getRexBuilder() ) );
    }


    public List<AlgNode> getRelationalEquivalent( List<AlgNode> values, List<AlgOptTable> entities, CatalogReader catalogReader ) {
        return List.of( getRelationalEquivalent() );
    }

}
