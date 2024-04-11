/*
 * Copyright 2019-2024 The Polypheny Project
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


import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.catalog.logistic.DataModel;


/**
 * Model for a namespace of the logical namespace.
 */
@Getter
@Value
public class Namespace {

    @JsonProperty
    String name;
    @JsonProperty
    DataModel type;
    @JsonProperty
    String store;

    // fields for creation
    @JsonProperty
    @NonFinal
    boolean create;
    @JsonProperty
    @NonFinal
    String authorization;

    // fields for deletion
    @JsonProperty
    @NonFinal
    boolean drop;
    @JsonProperty
    @NonFinal
    boolean cascade;


    /**
     * Constructor
     *
     * @param name name of the schema
     * @param type type of the schema, e.g. relational
     */
    public Namespace(
            @JsonProperty("name") String name,
            @JsonProperty("type") final DataModel type,
            @JsonProperty("store") @Nullable final String store ) {
        this.name = name;
        this.type = type;

        if ( type == DataModel.GRAPH ) {
            assert store != null;
            this.store = store;
        } else {
            assert store == null;
            this.store = null;
        }
    }

}
