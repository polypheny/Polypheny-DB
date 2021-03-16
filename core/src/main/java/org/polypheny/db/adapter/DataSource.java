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

package org.polypheny.db.adapter;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.polypheny.db.type.PolyType;

public abstract class DataSource extends Adapter {

    @Getter
    private final boolean dataReadOnly;


    protected DataSource( final int adapterId, final String uniqueName, final Map<String, String> settings, boolean dataReadOnly ) {
        super( adapterId, uniqueName, settings );
        this.dataReadOnly = dataReadOnly;

        informationPage.setLabel( "Sources" );
    }


    public abstract Map<String, List<ExportedColumn>> getExportedColumns();


    @AllArgsConstructor
    public static class ExportedColumn {

        public final String name;
        public final PolyType type;
        public final PolyType collectionsType;
        public final Integer length;
        public final Integer scale;
        public final Integer dimension;
        public final Integer cardinality;
        public final boolean nullable;
        public final String physicalSchemaName;
        public final String physicalTableName;
        public final String physicalColumnName;
        public final int physicalPosition;
        public final boolean primary;


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
