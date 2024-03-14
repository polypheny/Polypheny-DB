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


import java.util.List;
import java.util.StringJoiner;


public class TableConstraint {

    public String name;
    public String type;
    public boolean deferrable;
    public boolean initially_deferred;
    public String[] columns;


    public TableConstraint( final String name, final String type, List<String> columns ) {
        this.name = name;
        this.type = type;
        this.columns = columns.toArray( new String[0] );
    }


    public String[] asRow() {
        StringJoiner joiner = new StringJoiner( ", " );
        for ( String column : columns ) {
            joiner.add( column );
        }
        return new String[]{ this.name, this.type, joiner.toString() };
    }
}
