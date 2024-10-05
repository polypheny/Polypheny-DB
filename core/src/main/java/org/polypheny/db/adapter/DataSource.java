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

import com.google.gson.JsonObject;
import com.google.gson.JsonSerializer;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.pf4j.ExtensionPoint;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.type.PolyType;

@Getter
public abstract class DataSource<S extends AdapterCatalog> extends Adapter<S> implements ExtensionPoint {

    private final boolean dataReadOnly;


    protected DataSource( final long adapterId, final String uniqueName, final Map<String, String> settings, final DeployMode mode, boolean dataReadOnly, S catalog ) {
        super( adapterId, uniqueName, settings, mode, catalog );
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


    public static JsonSerializer<DataSource<?>> getSerializer() {
        //see https://futurestud.io/tutorials/gson-advanced-custom-serialization-part-1
        return ( src, typeOfSrc, context ) -> {
            JsonObject jsonSource = new JsonObject();
            jsonSource.addProperty( "adapterId", src.getAdapterId() );
            jsonSource.addProperty( "uniqueName", src.getUniqueName() );
            jsonSource.addProperty( "adapterName", src.getAdapterName() );
            jsonSource.add( "adapterSettings", context.serialize( AbstractAdapterSetting.serializeSettings( src.getAvailableSettings( src.getClass() ), src.getCurrentSettings() ) ) );
            jsonSource.add( "settings", context.serialize( src.getCurrentSettings() ) );
            jsonSource.add( "dataReadOnly", context.serialize( src.isDataReadOnly() ) );
            jsonSource.addProperty( "type", src.getAdapterType().name() );
            return jsonSource;
        };
    }


    private AdapterType getAdapterType() {
        return AdapterType.SOURCE;
    }

}
