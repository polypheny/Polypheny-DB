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
 * Schema for the index of a table
 */
@Getter
public class Index {

    private String schema;
    private String table;
    private String name;
    private String storeUniqueName;
    private String method;
    private String[] columns;


    public Index( final String schema, final String table, final String name, final String method, final String[] columns ) {
        this.schema = schema;
        this.table = table;
        this.name = name;
        this.method = method;
        this.columns = columns;
    }


    /**
     * Convert index to a row to display in the UI
     */
    public String[] asRow() {
        String[] row = new String[3];
        row[0] = this.name;
        row[1] = this.method;
        row[2] = String.join( ",", this.columns );
        return row;
    }

}
