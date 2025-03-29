/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.webui.schemaDiscovery.DataHandling;

/*
* Data structure for saving all schema information for every database collected because
* saving all database and schema information in two lists is more overhead
 */

import java.util.ArrayList;
import java.util.List;

public class DatabaseInfo {
    public String name;
    public List<SchemaInfo> schemas;

    public DatabaseInfo(String name) {
        this.name = name;
        this.schemas = new ArrayList<>();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("📦 Datenbank: ").append(name).append("\n");

        for (SchemaInfo schema : schemas) {
            sb.append("  📁 Schema: ").append(schema.name).append("\n");

            for (TableInfo table : schema.tables) {
                sb.append("    📄 Tabelle: ").append(table.name).append("\n");

                for (AttributeInfo attr : table.attributes) {
                    sb.append("      🔹 Attribut: ")
                            .append(attr.name)
                            .append(" : ")
                            .append(attr.type)
                            .append("\n");
                }
            }
        }

        return sb.toString();
    }
}

