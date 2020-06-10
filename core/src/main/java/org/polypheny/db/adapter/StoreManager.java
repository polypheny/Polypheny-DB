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

package org.polypheny.db.adapter;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.polypheny.db.adapter.Store.AdapterSetting;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogStore;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownStoreException;
import org.reflections.Reflections;


public class StoreManager {

    private static final StoreManager INSTANCE = new StoreManager();

    private final Map<Integer, Store> storesById = new HashMap<>();
    private final Map<String, Store> storesByName = new HashMap<>();

    public static StoreManager getInstance() {
        return INSTANCE;
    }


    private StoreManager() {
        // intentionally empty
    }


    public Store getStore( String uniqueName ) {
        uniqueName = uniqueName.toLowerCase();
        return storesByName.get( uniqueName );
    }


    public Store getStore( int id ) {
        return storesById.get( id );
    }


    public ImmutableMap<String, Store> getStores() {
        return ImmutableMap.copyOf( storesByName );
    }


    public List<AdapterInformation> getAvailableAdapters() {
        Reflections reflections = new Reflections( "org.polypheny.db" );
        Set<Class> classes = ImmutableSet.copyOf( reflections.getSubTypesOf( Store.class ) );
        List<AdapterInformation> result = new LinkedList<>();
        try {
            //noinspection unchecked
            for ( Class<Store> clazz : classes ) {
                // Exclude abstract classes
                if ( !Modifier.isAbstract( clazz.getModifiers() ) ) {
                    String name = (String) clazz.getDeclaredField( "ADAPTER_NAME" ).get( null );
                    String description = (String) clazz.getDeclaredField( "DESCRIPTION" ).get( null );
                    List<AdapterSetting> settings = (List<AdapterSetting>) clazz.getDeclaredField( "AVAILABLE_SETTINGS" ).get( null );
                    result.add( new AdapterInformation( name, description, clazz, settings ) );
                }
            }
        } catch ( NoSuchFieldException | IllegalAccessException e ) {
            throw new RuntimeException( "Something went wrong while retrieving list of available adapters.", e );
        }
        return result;
    }


    /**
     * Restores stores from catalog
     */
    public void restoreStores( Catalog catalog ) {
        try {
            List<CatalogStore> stores = catalog.getStores();
            for ( CatalogStore store : stores ) {
                Class<?> clazz = Class.forName( store.adapterClazz );
                Constructor<?> ctor = clazz.getConstructor( int.class, String.class, Map.class );
                Store instance = (Store) ctor.newInstance( store.id, store.uniqueName, store.settings );
                storesByName.put( instance.getUniqueName(), instance );
                storesById.put( instance.getStoreId(), instance );
            }
        } catch ( GenericCatalogException | NoSuchMethodException | ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException e ) {
            throw new RuntimeException( "Something went wrong while restoring stores from the catalog.", e );
        }
    }


    public Store addStore( Catalog catalog, String clazzName, String uniqueName, Map<String, String> settings ) {
        uniqueName = uniqueName.toLowerCase();
        if (storesByName.containsKey( uniqueName )) {
            throw new RuntimeException( "There is already a store with this unique name" );
        }
        Store instance;
        try {
            Class<?> clazz = Class.forName( clazzName );
            Constructor<?> ctor = clazz.getConstructor( int.class, String.class, Map.class );
            int storeId = catalog.addStore( uniqueName, clazzName, settings );
            instance = (Store) ctor.newInstance( storeId, uniqueName, settings );
            storesByName.put( instance.getUniqueName(), instance );
            storesById.put( instance.getStoreId(), instance );
        } catch ( ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException | GenericCatalogException e ) {
            throw new RuntimeException( "Something went wrong while adding a new store", e );
        }
        return instance;
    }


    public void removeStore( Catalog catalog, String uniqueName ) {
        uniqueName = uniqueName.toLowerCase();
        if (!storesByName.containsKey( uniqueName )) {
            throw new RuntimeException( "Unknown store: " + uniqueName );
        }
        try {
            CatalogStore catalogStore = catalog.getStore( uniqueName );

            // Check if the store has any placements
            List<CatalogColumnPlacement> placements = catalog.getColumnPlacementsOnStore( catalogStore.id );
            if (placements.size() != 0) {
                throw new RuntimeException( "There is still data placed on this store" );
            }

            // Shutdown store
            storesByName.get( uniqueName ).shutdown();

            // remove store from maps
            storesById.remove( catalogStore.id );
            storesByName.remove( uniqueName );

            // delete store from catalog
            catalog.deleteStore( catalogStore.id );
        } catch ( GenericCatalogException | UnknownStoreException e ) {
            throw new RuntimeException( "Something went wrong while removing a store", e );
        }
    }


    @AllArgsConstructor
    public static class AdapterInformation {
        public final String name;
        public final String description;
        public final Class clazz;
        public final List<AdapterSetting> settings;
    }

}
