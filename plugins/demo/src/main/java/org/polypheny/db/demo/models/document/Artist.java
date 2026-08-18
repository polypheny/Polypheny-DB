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

package org.polypheny.db.demo.models.document;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigInteger;
import java.util.List;

public class Artist {

    @JsonProperty("type-id")
    public String typeId;

    @JsonProperty("type")
    public String type;

    @JsonProperty("genres")
    public List<String> genres;

    @JsonProperty("annotation")
    public String annotation;

    @JsonProperty("id")
    public String id;

    @JsonProperty("isnis")
    public List<String> isnis;


    public class Rating {

        @JsonProperty("value")
        public float value;

        @JsonProperty("votes-count")
        public BigInteger votesCount;

    }


    @JsonProperty("rating")
    public Rating rating;

    @JsonProperty("disambiguation")
    public String disambiguation;

    @JsonProperty("name")
    public String name;

    @JsonProperty("area")
    public String area;

    @JsonProperty("begin-area")
    public String beginArea;

    @JsonProperty("gender-id")
    public int genderId;

    @JsonProperty("tags")
    public List<String> tags;

    @JsonProperty("aliases")
    public List<String> aliases;

    @JsonProperty("sort-name")
    public String sortName;

    @JsonProperty("gender")
    public String gender;

    @JsonProperty("ipis")
    public List<String> ipis;

    @JsonProperty("country")
    public String country;

    public class Lifespan {
        public String begin;
        public boolean ended;
        public String end;
    }

    @JsonProperty("life-span")
    public Lifespan lifeSpan;

    @JsonProperty("end-area")
    public String endArea;

    public class Relation {
        @JsonProperty("attribute-values")
        public String attributeValues;

        @JsonProperty("end")
        public String end;

        @JsonProperty("source-credit")
        public String sourceCredit;

        @JsonProperty("ended")
        public boolean ended;

        @JsonProperty("targetType")
        public String targetType;
    }

    //@JsonProperty("relations")
    //public String relations;
}
