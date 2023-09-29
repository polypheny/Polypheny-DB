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

package org.polypheny.db.webui.models;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import lombok.Getter;
import org.polypheny.db.catalog.logistic.NamespaceType;


/**
 * Model for a namespace of the logical namespace.
 */
@Getter
public class Namespace {

    @JsonSerialize
    private String name;
    @JsonSerialize
    private NamespaceType type;
    @JsonSerialize
    private final String store;

    // fields for creation
    @JsonSerialize
    private boolean create;
    @JsonSerialize
    private String authorization;

    // fields for deletion
    @JsonSerialize
    private boolean drop;
    @JsonSerialize
    private boolean cascade;


    /**
     * Constructor
     *
     * @param name name of the schema
     * @param type type of the schema, e.g. relational
     */
    public Namespace( final @JsonProperty("name") String name, @JsonProperty("type") final NamespaceType type, @JsonProperty("store") @Nullable final String store ) {
        this.name = name;
        this.type = type;

        if ( type == NamespaceType.GRAPH ) {
            assert store != null;
            this.store = store;
        } else {
            assert store == null;
            this.store = null;
        }
    }

}
