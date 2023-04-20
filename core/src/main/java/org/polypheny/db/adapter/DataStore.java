/*
 * Copyright 2019-2023 The Polypheny Project
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


import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.ExtensionPoint;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.StoreCatalog;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.type.PolyType;

@Slf4j
public abstract class DataStore<S extends StoreCatalog> extends Adapter<S> implements ExtensionPoint {

    @Getter
    private final boolean persistent;

    protected final transient Catalog catalog = Catalog.getInstance();


    public DataStore( final long adapterId, final String uniqueName, final Map<String, String> settings, final boolean persistent ) {
        super( adapterId, uniqueName, settings );
        this.persistent = persistent;

        informationPage.setLabel( "Stores" );
    }


    public List<NamespaceType> getSupportedSchemaType() {
        log.info( "Using default NamespaceType support." );
        return ImmutableList.of( NamespaceType.RELATIONAL );
    }

    //public abstract void createTable( StoreCatalog snapshot, Context context, AllocationTable alloc );


    public abstract void addColumn( S snapshot, Context context, long allocId, LogicalColumn column );

    public abstract void dropColumn( S snapshot, Context context, long allocId, long columnId );

    public abstract String addIndex( S snapshot, Context context, LogicalIndex logicalIndex, AllocationTable allocation );

    public abstract void dropIndex( S snapshot, Context context, LogicalIndex logicalIndex, List<Long> partitionIds );

    public abstract void updateColumnType( S snapshot, Context context, long allocId, long columnId, PolyType oldType );

    public abstract List<AvailableIndexMethod> getAvailableIndexMethods();

    public abstract AvailableIndexMethod getDefaultIndexMethod();

    public abstract List<FunctionalIndexInfo> getFunctionalIndexes( LogicalTable catalogTable );




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
                // columnNames.add( Catalog.getInstance().getLogicalRel( columnNames ).getColumn( columnId ).name );
                // todo dl
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
