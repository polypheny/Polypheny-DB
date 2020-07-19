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


import java.util.HashMap;
import lombok.Setter;
import lombok.experimental.Accessors;


@Accessors(chain = true)
public class HubMeta {

    private final String schema;
    private int numberOfRows;
    @Setter
    private long fileSize;
    private final HashMap<String, TableMapping> tables = new HashMap<>();

    public HubMeta( String schema ) {
        this.schema = schema;
    }

    public HubMeta addTable( final String table, final int numberOfRows ) {
        this.tables.put( table, new TableMapping( table ) );
        this.numberOfRows += numberOfRows;
        return this;
    }

    public static class TableMapping {

        public String initialName;
        public String newName;

        public TableMapping( final String initialName ) {
            this.initialName = initialName;
        }
    }
}
