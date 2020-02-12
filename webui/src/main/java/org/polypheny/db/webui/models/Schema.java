/*
 * Copyright 2019-2020 The Polypheny Project
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


import lombok.Getter;


/**
 * Model for a schema of a DBMS
 */
@Getter
public class Schema {

    private String name;
    private String type; // TODO: enum

    // fields for creation
    private boolean create;
    private String authorization;

    // fields for deletion
    private boolean drop;
    private boolean cascade;


    /**
     * Constructor
     *
     * @param name name of the schema
     * @param type type of the schema, e.g. relational
     */
    public Schema( final String name, final String type ) {
        this.name = name;
        this.type = type;
    }

}
