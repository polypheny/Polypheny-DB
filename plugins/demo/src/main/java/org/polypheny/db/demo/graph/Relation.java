/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.demo.graph;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Relation extends CypherObject {
    @Override
    String query() {
        this.addQueryPart( this.sourceCredit );
        this.addQueryPart( this.artist );
        this.addQueryPart( this.end );
        this.addQueryPart( this.targetCredit );
        this.addQueryPart( this.begin );
        this.addQueryPart( this.targetType );
        this.addQueryPart( this.typeId );
        this.addQueryPart( this.attributes );
        this.addQueryPart( this.direction );
        this.addQueryPart( this.attributeValues );

        return "CREATE";
    }


    @JsonProperty("source-credit")
    public Field<String> sourceCredit = new Field<>( "source-credit", String.class );

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RelationArtist extends CypherObject {
        @JsonProperty("name")
        private Field<String> name = new Field<>( "name", String.class );
        @JsonProperty("type")
        private Field<String> type = new Field<>( "type", String.class );
        @JsonProperty("sort-name")
        private Field<String> sortName = new Field<>( "sort-name", String.class );
        @JsonProperty("disambiguation")
        private Field<String> disambiguation = new Field<>( "disambiguation", String.class );
        @JsonProperty("country")
        private Field<String> country = new Field<>( "country", String.class );
        @JsonProperty("id")
        private Field<String> id = new Field<>( "id", String.class );
        @JsonProperty("type-id")
        private Field<String> typeId = new Field<>( "type-id", String.class );

        @Override
        public String query() {
            this.addQueryPart( this.name );
            this.addQueryPart( this.type );
            this.addQueryPart( this.sortName );
            this.addQueryPart( this.disambiguation );
            this.addQueryPart( this.country );
            this.addQueryPart( this.id );
            this.addQueryPart( this.typeId );

            return String.format( "{ %s }", this.getQueryPart() );
        }
    }

    @JsonProperty("artist")
    public Field<RelationArtist> artist = new Field<>( "artist", RelationArtist.class );

    @JsonProperty("end")
    public Field<String> end = new Field<>( "end", String.class );

    @JsonProperty("target-credit")
    public Field<String> targetCredit = new Field<>( "target-credit", String.class );

    @JsonProperty("begin")
    public Field<String> begin = new Field<>( "begin", String.class );

    @JsonProperty("target-type")
    public Field<String> targetType = new Field<>( "target-type", String.class );

    @JsonProperty("type-id")
    public Field<String> typeId = new Field<>( "type-id", String.class );

    @JsonProperty("attributes")
    public Field<List<String>> attributes = new Field<>( "attributes", new TypeReference<>() {} );

    @JsonProperty("direction")
    public Field<String> direction = new Field<>( "direction", String.class );

    @JsonProperty("attribute-values")
    public Field<Map<String, String>> attributeValues = new Field<>( "attribute-values", new TypeReference<>() {} );

}
