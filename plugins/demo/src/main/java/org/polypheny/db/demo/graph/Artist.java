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
e */

package org.polypheny.db.demo.graph;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@JsonIgnoreProperties(ignoreUnknown = true)
public class Artist extends CypherObject {
    @Override
    public String query() {
        this.addQueryPart( this.id );
        this.addQueryPart( this.tags );
        //this.addQueryPart( this.annotation );
        this.addQueryPart( this.genderId );
        this.addQueryPart( this.lifeSpan );
        this.addQueryPart( this.type );
        this.addQueryPart( this.ipis );
        this.addQueryPart( this.rating );
        this.addQueryPart( this.name );
        this.addQueryPart( this.area );
        this.addQueryPart( this.isnis );
        this.addQueryPart( this.beginArea );
        this.addQueryPart( this.typeId );
        this.addQueryPart( this.gender );
        this.addQueryPart( this.country );
        this.addQueryPart( this.genres );
        this.addQueryPart( this.endArea );

        return String.format( "CREATE (n:Artist {%s})", this.getQueryPart() );
    }

    @JsonProperty("id")
    public Field<String> id = new Field<>( "id", String.class );

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Tag extends CypherObject {
        @JsonProperty("count")
        private Field<Integer> count = new Field<>( "count", Integer.class );
        @JsonProperty("name")
        private Field<String> name = new Field<>( "name", String.class );


        @Override
        public String query() {
            this.addQueryPart( this.count );
            this.addQueryPart( this.name );

            return String.format("{ %s }", this.getQueryPart() );
        }
    }

    @JsonProperty("tags")
    private Field<List<Tag>> tags = new Field<>( "tags", new TypeReference<>() {} );

    //@JsonProperty("annotation")
    //private Field<String> annotation = new Field<>( "annotation", String.class );

    @JsonProperty("gender-id")
    private Field<String> genderId = new Field<>( "gender_id", String.class );


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class LifeSpan extends CypherObject {
        @JsonProperty("ended")
        private Field<Boolean> ended = new Field<>( "ended", Boolean.class );
        @JsonProperty("end")
        private Field<String> end = new Field<>( "end", String.class );
        @JsonProperty("begin")
        private Field<String> begin = new Field<>( "begin", String.class );

        @Override
        public String query() {
            this.addQueryPart( this.ended );
            this.addQueryPart( this.end );
            this.addQueryPart( this.begin );
            return String.format( "{ %s }", this.getQueryPart() );
        }

    }

    @JsonProperty("life-span")
    private Field<LifeSpan> lifeSpan = new Field<>( "life_span", new TypeReference<>() {} );

    @JsonProperty("type")
    private Field<String> type = new Field<>( "type", String.class );

    @JsonProperty("ipis")
    private Field<List<String>> ipis = new Field<>( "ipis", new TypeReference<>() {} );


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Rating extends CypherObject {
        @JsonProperty("value")
        private Field<Double> value = new Field<>( "value", Double.class );

        @JsonProperty("votes-count")
        private Field<Integer> votesCount = new Field<>( "votes_count", Integer.class );


        @Override
        public String query() {
            this.addQueryPart( this.value );
            this.addQueryPart( this.votesCount );

            return String.format( "{ %s }", this.getQueryPart() );
        }
    }

    @JsonProperty("rating")
    private Field<Rating> rating = new Field<>( "rating", Rating.class );

    @JsonProperty("name")
    private Field<String> name = new Field<>( "name", String.class );


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Place extends CypherObject {
        @JsonProperty("id")
        private Field<String> id = new Field<>( "id", String.class );

        @JsonProperty("type-id")
        private Field<String> typeId = new Field<>( "type_id", String.class );

        @JsonProperty("iso-3166-1-codes")
        private Field<List<String>> iso3166_1Codes = new Field<>( "iso_3166_1_codes", new TypeReference<>(){} );

        @JsonProperty("disambiguation")
        private Field<String> disambiguation = new Field<>( "disambiguation", String.class );

        @JsonProperty("name")
        private Field<String> name = new Field<>( "name", String.class );

        @JsonProperty("type")
        private Field<String> type = new Field<>( "type", String.class );

        @JsonProperty("sort-name")
        private Field<String> sortName = new Field<>( "sort_name", String.class );


        @Override
        public String query() {
            this.addQueryPart( this.id );
            this.addQueryPart( this.typeId );
            this.addQueryPart( this.iso3166_1Codes );
            this.addQueryPart( this.disambiguation );
            this.addQueryPart( this.name );
            this.addQueryPart( this.type );
            this.addQueryPart( this.sortName );

            return String.format( "{ %s }", this.getQueryPart() );
        }
    }

    @JsonProperty("area")
    private Field<Place> area = new Field<>( "area", Place.class );

    @JsonProperty("isnis")
    private Field<List<String>> isnis = new Field<>( "isnis", new TypeReference<>(){} );

    @JsonProperty("begin-area")
    private Field<Place> beginArea = new Field<>( "begin_area", Place.class );

    @JsonProperty("type-id")
    private Field<String> typeId = new Field<>( "type_id", String.class );

    @JsonProperty("gender")
    private Field<String> gender = new Field<>( "gender", String.class );

    @JsonProperty("country")
    private Field<String> country = new Field<>( "country", String.class );


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Genre extends CypherObject {
        @JsonProperty("id")
        private Field<String> id = new Field<>( "id", String.class );
        @JsonProperty("name")
        private Field<String> name = new Field<>( "name", String.class );
        @JsonProperty("count")
        private Field<Integer> count = new Field<>( "count", Integer.class );
        @JsonProperty("disambiguation")
        private Field<String> disambiguation = new Field<>( "disambiguation", String.class );


        @Override
        public String query() {
            this.addQueryPart( this.id );
            this.addQueryPart( this.name );
            this.addQueryPart( this.count );
            this.addQueryPart( this.disambiguation );

            return String.format( "{ %s }", this.getQueryPart() );
        }
    }

    @JsonProperty("genres")
    private Field<List<Genre>> genres = new Field<>( "genres", new TypeReference<>(){} );

    @JsonProperty("end-area")
    private Field<Place> endArea = new Field<>( "end_area", Place.class );
}
