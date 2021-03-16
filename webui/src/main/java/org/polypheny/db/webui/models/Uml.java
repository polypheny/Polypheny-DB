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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * Stores information that is needed for the UML view, such as the list of all tables of a schema with their columns and a list of
 * all the foreign keys of a schema
 */
public class Uml {

    private Map<String, DbTable> tables = new HashMap<>();
    private ForeignKey[] foreignKeys;


    public Uml( final ArrayList<DbTable> tables, final ArrayList<ForeignKey> foreignKeys ) {
        for ( DbTable table : tables ) {
            this.tables.put( table.getTableName(), table );
        }
        this.foreignKeys = foreignKeys.toArray( new ForeignKey[0] );
    }

}
