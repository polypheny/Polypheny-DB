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


import com.google.gson.JsonObject;
import com.google.gson.JsonSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.type.PolyType;


public abstract class DataStore extends Adapter {

    @Getter
    private final boolean persistent;

    protected final transient Catalog catalog = Catalog.getInstance();


    public DataStore( final int adapterId, final String uniqueName, final Map<String, String> settings, final boolean persistent ) {
        super( adapterId, uniqueName, settings );
        this.persistent = persistent;

        informationPage.setLabel( "Stores" );
    }


    public abstract void createTable( Context context, CatalogTable combinedTable, List<Long> partitionIds );

    public abstract void dropTable( Context context, CatalogTable combinedTable, List<Long> partitionIds );

    public abstract void addColumn( Context context, CatalogTable catalogTable, CatalogColumn catalogColumn );

    public abstract void dropColumn( Context context, CatalogColumnPlacement columnPlacement );

    public abstract void addIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds );

    public abstract void dropIndex( Context context, CatalogIndex catalogIndex, List<Long> partitionIds );

    public abstract void updateColumnType( Context context, CatalogColumnPlacement columnPlacement, CatalogColumn catalogColumn, PolyType oldType );

    public abstract List<AvailableIndexMethod> getAvailableIndexMethods();

    public abstract AvailableIndexMethod getDefaultIndexMethod();

    public abstract List<FunctionalIndexInfo> getFunctionalIndexes( CatalogTable catalogTable );


    @AllArgsConstructor
    public static class AvailableIndexMethod {

        public final String name;
        public final String displayName;

    }


    @AllArgsConstructor
    public static class FunctionalIndexInfo {

        public final List<Long> columnIds;
        public final String methodDisplayName;


        public List<String> getColumnNames() {
            List<String> columnNames = new ArrayList<>( columnIds.size() );
            for ( long columnId : columnIds ) {
                columnNames.add( Catalog.getInstance().getColumn( columnId ).name );
            }
            return columnNames;
        }

    }


    public static JsonSerializer<DataStore> getSerializer() {
        //see https://futurestud.io/tutorials/gson-advanced-custom-serialization-part-1
        return ( src, typeOfSrc, context ) -> {
            JsonObject jsonStore = new JsonObject();
            jsonStore.addProperty( "adapterId", src.getAdapterId() );
            jsonStore.addProperty( "uniqueName", src.getUniqueName() );
            jsonStore.add( "adapterSettings", context.serialize( AbstractAdapterSetting.serializeSettings( src.getAvailableSettings(), src.getCurrentSettings() ) ) );
            jsonStore.add( "currentSettings", context.serialize( src.getCurrentSettings() ) );
            jsonStore.addProperty( "adapterName", src.getAdapterName() );
            jsonStore.addProperty( "type", src.getClass().getCanonicalName() );
            jsonStore.add( "persistent", context.serialize( src.isPersistent() ) );
            jsonStore.add( "availableIndexMethods", context.serialize( src.getAvailableIndexMethods() ) );
            return jsonStore;
        };
    }

}

