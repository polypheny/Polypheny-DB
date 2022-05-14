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
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;


public abstract class DocumentsValues extends Values {

    @Getter
    public final ImmutableList<BsonValue> documentTuples;


    public DocumentsValues( AlgOptCluster cluster, AlgTraitSet traitSet, AlgDataType rowType, ImmutableList<BsonValue> documentTuples ) {
        super( cluster, rowType, validateLiterals( normalize( documentTuples, cluster.getRexBuilder() ), rowType, cluster.getRexBuilder() ), traitSet );
        //super( cluster, traitSet );
        this.rowType = rowType;
        this.documentTuples = validate( documentTuples, rowType );
    }


    protected static ImmutableList<BsonValue> validate( ImmutableList<BsonValue> tuples, AlgDataType defaultRowType ) {
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


    protected static ImmutableList<ImmutableList<RexLiteral>> normalize( List<BsonValue> tuples, RexBuilder rexBuilder ) {
        List<ImmutableList<RexLiteral>> normalized = new ArrayList<>();

        JsonWriterSettings writerSettings = JsonWriterSettings.builder().outputMode( JsonMode.STRICT ).build();

        for ( BsonValue tuple : tuples ) {
            List<RexLiteral> normalizedTuple = new ArrayList<>();
            String id = ObjectId.get().toString();
            if ( tuple.isDocument() && tuple.asDocument().containsKey( "_id" ) ) {
                id = tuple.asDocument().get( "_id" ).toString();
                tuple.asDocument().remove( "_id" );
            }

            normalizedTuple.add( 0, rexBuilder.makeLiteral( id ) );
            String parsed = tuple.asDocument().toJson( writerSettings );
            normalizedTuple.add( 1, rexBuilder.makeLiteral( parsed ) );
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


    @Override
    public String algCompareString() {
        return getClass().getCanonicalName() + "$" + documentTuples.hashCode() + "$";
    }

}
