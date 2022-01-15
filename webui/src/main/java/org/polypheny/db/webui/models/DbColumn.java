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

package org.polypheny.db.webui.models;


import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import lombok.Setter;
import lombok.experimental.Accessors;


/**
 * Information about a column of a table for the header of a table in the UI
 */
@Accessors(chain = true)
public class DbColumn {

    public String name;
    @Setter
    public String physicalName;

    // for both
    public String dataType; //varchar/int/etc
    public String collectionsType;

    // for the Data-Table in the UI
    public SortState sort;
    public String filter;

    // for editing columns
    public boolean primary;
    public boolean nullable;
    public Integer precision;
    public Integer scale;
    public String defaultValue;
    public Integer dimension;
    public Integer cardinality;

    //for data source columns
    public String as;


    public DbColumn( final String name ) {
        this.name = name;
    }


    public DbColumn(
            final String name,
            final String dataType,
            final boolean nullable,
            final Integer precision,
            final SortState sort,
            final String filter ) {
        this.name = name;
        this.dataType = dataType;
        this.nullable = nullable;
        if ( dataType.equals( "varchar" ) ) {
            this.precision = precision;
        }
        this.sort = sort;
        this.filter = filter;
    }


    public DbColumn(
            final String name,
            final String dataType,
            final String collectionsType,
            final boolean nullable,
            final Integer precision,
            final Integer scale,
            final Integer dimension,
            final Integer cardinality,
            final boolean primary,
            final String defaultValue ) {
        this.name = name;
        this.dataType = dataType;
        this.collectionsType = collectionsType;
        this.nullable = nullable;
        this.precision = precision;
        this.scale = scale;
        this.dimension = dimension;
        this.cardinality = cardinality;
        this.primary = primary;
        this.defaultValue = defaultValue;
    }


    public DbColumn(
            final String name,
            final String dataType,
            final String collectionsType,
            final boolean nullable,
            final Integer precision,
            final Integer scale,
            final Integer dimension,
            final Integer cardinality,
            final boolean primary,
            final String defaultValue,
            final SortState sort,
            final String filter
    ) {
        this( name, dataType, collectionsType, nullable, precision, scale, dimension, cardinality, primary, defaultValue );
        this.sort = sort;
        this.filter = filter;
    }


    private DbColumn( JsonReader in ) throws IOException {
        while ( in.peek() != JsonToken.END_OBJECT ) {
            switch ( in.nextName() ) {
                case "name":
                    name = in.nextString();
                    break;
                case "dataType":
                    dataType = in.nextString();
                    break;
                case "collectionsType":
                    collectionsType = in.nextString();
                    break;
                case "nullable":
                    nullable = in.nextBoolean();
                    break;
                case "precision":
                    precision = handleInteger( in );
                    break;
                case "scale":
                    scale = handleInteger( in );
                    break;
                case "dimension":
                    dimension = handleInteger( in );
                    break;
                case "cardinality":
                    cardinality = handleInteger( in );
                    break;
                case "primary":
                    primary = in.nextBoolean();
                    break;
                case "defaultValue":
                    defaultValue = in.nextString();
                    break;
                case "sort":
                    sort = SortState.getSerializer().read( in );
                    break;
                case "filter":
                    filter = in.nextString();
                    break;
                default:
                    throw new RuntimeException( "There was an unrecognized column while deserializing DbColumn." );
            }
        }
    }


    private Integer handleInteger( JsonReader in ) throws IOException {
        if ( in.peek() == JsonToken.NULL ) {
            in.nextNull();
            return null;
        } else {
            return in.nextInt();
        }
    }


    public static TypeAdapter<DbColumn> getSerializer() {
        return new TypeAdapter<DbColumn>() {
            @Override
            public void write( JsonWriter out, DbColumn col ) throws IOException {
                out.beginObject();
                out.name( "name" );
                out.value( col.name );
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
                DbColumn column = new DbColumn( in );
                in.endObject();

                return column;
            }
        };
    }

}
