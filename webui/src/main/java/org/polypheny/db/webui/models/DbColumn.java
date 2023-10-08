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

package org.polypheny.db.webui.models;


import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.webui.models.catalog.FieldDefinition;


/**
 * Information about a column of a table for the header of a table in the UI
 */
@Accessors(chain = true)
@SuperBuilder
public class DbColumn extends FieldDefinition {

    // for the Data-Table in the UI
    public SortState sort;
    public String filter;
    @Setter
    public String physicalName;

    // for editing columns
    public boolean primary;
    public boolean nullable;
    public Integer precision;
    public Integer scale;
    public String defaultValue;
    public Integer dimension;
    public Integer cardinality;
    public String collectionsType;

    //for data source columns
    public String as;


    private static DbColumn create( JsonReader in ) throws IOException {
        DbColumnBuilder<?, ?> builder = DbColumn.builder();
        while ( in.peek() != JsonToken.END_OBJECT ) {
            switch ( in.nextName() ) {
                case "name":
                    builder.name( in.nextString() );
                    break;
                case "physicalName":
                    builder.physicalName( in.nextString() );
                    break;
                case "dataType":
                    builder.dataType( in.nextString() );
                    break;
                case "collectionsType":
                    builder.collectionsType( in.nextString() );
                    break;
                case "nullable":
                    builder.nullable( in.nextBoolean() );
                    break;
                case "precision":
                    builder.precision( handleInteger( in ) );
                    break;
                case "scale":
                    builder.scale( handleInteger( in ) );
                    break;
                case "dimension":
                    builder.dimension( handleInteger( in ) );
                    break;
                case "cardinality":
                    builder.cardinality( handleInteger( in ) );
                    break;
                case "primary":
                    builder.primary( in.nextBoolean() );
                    break;
                case "defaultValue":
                    builder.defaultValue( in.nextString() );
                    break;
                case "sort":
                    builder.sort( SortState.getSerializer().read( in ) );
                    break;
                case "filter":
                    builder.filter( in.nextString() );
                    break;
                default:
                    throw new RuntimeException( "There was an unrecognized column while deserializing DbColumn." );
            }
        }
        return builder.build();

    }


    private static Integer handleInteger( JsonReader in ) throws IOException {
        if ( in.peek() == JsonToken.NULL ) {
            in.nextNull();
            return null;
        } else {
            return in.nextInt();
        }
    }


    public static TypeAdapter<DbColumn> serializer = new TypeAdapter<>() {
        @Override
        public void write( JsonWriter out, DbColumn col ) throws IOException {
            out.beginObject();
            out.name( "name" );
            out.value( col.name );
            out.name( "physicalName" );
            out.value( col.physicalName );
            out.name( "dataType" );
            out.value( col.dataType );
            out.name( "collectionsType" );
            out.value( col.collectionsType );
            out.name( "nullable" );
            out.value( col.nullable );
            out.name( "precision" );
            out.value( col.precision );
            out.name( "scale" );
            out.value( col.scale );
            out.name( "dimension" );
            out.value( col.dimension );
            out.name( "cardinality" );
            out.value( col.cardinality );
            out.name( "primary" );
            out.value( col.primary );
            out.name( "defaultValue" );
            out.value( col.defaultValue );
            out.name( "sort" );
            SortState.getSerializer().write( out, col.sort );
            out.name( "filter" );
            out.value( col.filter );
            out.endObject();
        }


        @Override
        public DbColumn read( JsonReader in ) throws IOException {
            if ( in.peek() == null ) {
                in.nextNull();
                return null;
            }
            in.beginObject();
            DbColumn column = DbColumn.create( in );
            in.endObject();

            return column;
        }

    };

}
