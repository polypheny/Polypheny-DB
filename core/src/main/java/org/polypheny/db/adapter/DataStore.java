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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.ExtensionPoint;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.logical.LogicalTable;

@Slf4j
public abstract class DataStore<S extends AdapterCatalog> extends Adapter<S> implements Modifiable, ExtensionPoint {

    @Getter
    private final boolean persistent;

    protected final transient Catalog catalog = Catalog.getInstance();


    public DataStore( final long adapterId, final String uniqueName, final Map<String, String> settings, final boolean persistent, S storeCatalog ) {
        super( adapterId, uniqueName, settings, storeCatalog );
        this.persistent = persistent;

        informationPage.setLabel( "Stores" );
    }


    public abstract List<IndexMethodModel> getAvailableIndexMethods();

    public abstract IndexMethodModel getDefaultIndexMethod();

    public abstract List<FunctionalIndexInfo> getFunctionalIndexes( LogicalTable catalogTable );


    public record IndexMethodModel(String name, String displayName) {

    }


    public record FunctionalIndexInfo(List<Long> columnIds, String methodDisplayName) {

        public List<String> getColumnNames() {
            List<String> columnNames = new ArrayList<>( columnIds.size() );
            for ( long columnId : columnIds ) {
                columnNames.add( Catalog.getInstance().getSnapshot().rel().getColumn( columnId ).orElseThrow().name );
            }
            return columnNames;
        }

    }


    public static JsonSerializer<DataStore<?>> getSerializer() {
        //see https://futurestud.io/tutorials/gson-advanced-custom-serialization-part-1
        return ( src, typeOfSrc, context ) -> {
            JsonObject jsonStore = new JsonObject();
            jsonStore.addProperty( "adapterId", src.getAdapterId() );
            jsonStore.add( "adapterSettings", context.serialize( AbstractAdapterSetting.serializeSettings( src.getAvailableSettings( src.getClass() ), src.getCurrentSettings() ) ) );
            jsonStore.add( "currentSettings", context.serialize( src.getCurrentSettings() ) );
            jsonStore.addProperty( "adapterName", src.getAdapterName() );
            jsonStore.addProperty( "uniqueName", src.getUniqueName() );
            jsonStore.addProperty( "type", src.getAdapterType().name() );
            jsonStore.add( "persistent", context.serialize( src.isPersistent() ) );
            jsonStore.add( "availableIndexMethods", context.serialize( src.getAvailableIndexMethods() ) );
            return jsonStore;
        };
    }


    private AdapterType getAdapterType() {
        return AdapterType.STORE;
    }

}
