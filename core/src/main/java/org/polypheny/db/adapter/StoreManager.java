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
import java.util.Map.Entry;
import java.util.Set;
import org.polypheny.db.adapter.Adapter.AdapterSetting;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.reflections.Reflections;


public class StoreManager extends AdapterManager {

    private static final StoreManager INSTANCE = new StoreManager();


    public static StoreManager getInstance() {
        return INSTANCE;
    }


    private StoreManager() {
        // intentionally empty
    }


    public DataStore getStore( String uniqueName ) {
        Adapter adapter = getAdapter( uniqueName );
        if ( adapter instanceof DataStore ) {
            return (DataStore) adapter;
        }
        return null;
    }


    public DataStore getStore( int id ) {
        Adapter adapter = getAdapter( id );
        if ( adapter instanceof DataStore ) {
            return (DataStore) adapter;
        }
        return null;
    }


    public ImmutableMap<String, DataStore> getStores() {
        Map<String, DataStore> map = new HashMap<>();
        for ( Entry<String, Adapter> entry : getAdapters().entrySet() ) {
            if ( entry.getValue() instanceof DataStore ) {
                map.put( entry.getKey(), (DataStore) entry.getValue() );
            }
        }
        return ImmutableMap.copyOf( map );
    }


    public List<AdapterInformation> getAvailableStoreAdapters() {
        Reflections reflections = new Reflections( "org.polypheny.db" );
        Set<Class> classes = ImmutableSet.copyOf( reflections.getSubTypesOf( DataStore.class ) );
        List<AdapterInformation> result = new LinkedList<>();
        try {
            //noinspection unchecked
            for ( Class<DataStore> clazz : classes ) {
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


    public DataStore addStore( Catalog catalog, String clazzName, String uniqueName, Map<String, String> settings ) {
        uniqueName = uniqueName.toLowerCase();
        if ( getAdapters().containsKey( uniqueName ) ) {
            throw new RuntimeException( "There is already an adapter with this unique name" );
        }
        DataStore instance;
        try {
            Class<?> clazz = Class.forName( clazzName );
            Constructor<?> ctor = clazz.getConstructor( int.class, String.class, Map.class );
            int storeId = catalog.addAdapter( uniqueName, clazzName, AdapterType.STORE, settings );
            instance = (DataStore) ctor.newInstance( storeId, uniqueName, settings );
            addAdapter( instance );
        } catch ( ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e ) {
            throw new RuntimeException( "Something went wrong while adding a new store", e );
        }
        return instance;
    }


    public void removeStore( Catalog catalog, String uniqueName ) {
        uniqueName = uniqueName.toLowerCase();
        DataStore storeInstance = getStore( uniqueName );
        if ( storeInstance == null ) {
            throw new RuntimeException( "Unknown data store: " + uniqueName );
        }
        try {
            CatalogAdapter catalogAdapter = catalog.getAdapter( uniqueName );

            // Check if the store has any placements
            List<CatalogColumnPlacement> placements = catalog.getColumnPlacementsOnAdapter( catalogAdapter.id );
            if ( placements.size() != 0 ) {
                throw new RuntimeException( "There is still data placed on this data store" );
            }

            // Shutdown store
            storeInstance.shutdown();

            // remove store from maps
            removeAdapter( storeInstance );

            // delete store from catalog
            catalog.deleteAdapter( catalogAdapter.id );
        } catch ( UnknownAdapterException e ) {
            throw new RuntimeException( "Something went wrong while removing a data store", e );
        }
    }

}
