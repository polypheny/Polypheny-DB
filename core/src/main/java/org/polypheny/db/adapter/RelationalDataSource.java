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

package org.polypheny.db.adapter;

import java.util.List;
import java.util.Map;
import org.polypheny.db.type.PolyType;

public interface RelationalDataSource {

    Map<String, List<ExportedColumn>> getExportedColumns();

    record ExportedColumn( String name, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality, boolean nullable, String physicalSchemaName, String physicalTableName, String physicalColumnName, int physicalPosition, boolean primary ) {

        public String getDisplayType() {
            String typeStr = type.getName();
            if ( scale != null ) {
                typeStr += "(" + length + "," + scale + ")";
            } else if ( length != null ) {
                typeStr += "(" + length + ")";
            }

            if ( collectionsType != null ) {
                typeStr += " " + collectionsType.getName();
                if ( cardinality != null ) {
                    typeStr += "(" + dimension + "," + cardinality + ")";
                } else if ( dimension != null ) {
                    typeStr += "(" + dimension + ")";
                }
            }
            return typeStr;
        }

    }

}
