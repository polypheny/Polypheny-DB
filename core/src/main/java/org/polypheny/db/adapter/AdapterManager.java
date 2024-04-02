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

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalAdapter;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.entity.allocation.AllocationEntity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;


public class AdapterManager {

    public static Expression ADAPTER_MANAGER_EXPRESSION = Expressions.call( AdapterManager.class, "getInstance" );

    private final Map<Long, Adapter<?>> adapterById = new HashMap<>();
    private final Map<String, Adapter<?>> adapterByName = new HashMap<>();


    private static final AdapterManager INSTANCE = new AdapterManager();


    public static AdapterManager getInstance() {
        return INSTANCE;
    }


    private AdapterManager() {
        // intentionally empty
    }


    public static long addAdapterTemplate( Class<? extends Adapter<?>> clazz, String adapterName, Function4<Long, String, Map<String, String>, Adapter<?>> deployer ) {
        List<AbstractAdapterSetting> settings = AdapterTemplate.getAllSettings( clazz );
        AdapterProperties properties = clazz.getAnnotation( AdapterProperties.class );
        return Catalog.getInstance().createAdapterTemplate( clazz, adapterName, properties.description(), List.of( properties.usedModes() ), settings, deployer );
    }


    public static void removeAdapterTemplate( long templateId ) {
        AdapterTemplate template = Catalog.snapshot().getAdapterTemplate( templateId ).orElseThrow();
        if ( Catalog.getInstance().getSnapshot().getAdapters().stream().anyMatch( a -> a.adapterName.equals( template.adapterName ) && a.type == template.adapterType ) ) {
            throw new GenericRuntimeException( "Adapter is still deployed!" );
        }
        Catalog.getInstance().dropAdapterTemplate( templateId );
    }


    public static AdapterTemplate getAdapterTemplate( String name, AdapterType adapterType ) {
        return Catalog.snapshot().getAdapterTemplate( name, adapterType ).orElseThrow();
    }


    public List<AdapterInformation> getAdapterTemplates( AdapterType adapterType ) {
        List<AdapterTemplate> adapterTemplates = Catalog.snapshot().getAdapterTemplates( adapterType );

        List<AdapterInformation> result = new ArrayList<>();

        for ( AdapterTemplate adapterTemplate : adapterTemplates ) {
            // Exclude abstract classes
            if ( !Modifier.isAbstract( adapterTemplate.getClazz().getModifiers() ) ) {

                AdapterProperties properties = adapterTemplate.getClazz().getAnnotation( AdapterProperties.class );
                if ( properties == null ) {
                    throw new GenericRuntimeException( adapterTemplate.getClazz().getSimpleName() + " does not annotate the adapter correctly" );
                }
                // Merge annotated AdapterSettings into settings
                List<AbstractAdapterSetting> settings = AbstractAdapterSetting.fromAnnotations( adapterTemplate.getClazz().getAnnotations(), adapterTemplate.getClazz().getAnnotation( AdapterProperties.class ) );

                result.add( new AdapterInformation( properties.name(), properties.description(), adapterType, settings, List.of( properties.usedModes() ) ) );
            }
        }

        return result;
    }


    @NotNull
    public Optional<Adapter<?>> getAdapter( String uniqueName ) {
        return Optional.ofNullable( adapterByName.get( uniqueName.toLowerCase() ) );
    }


    @NotNull
    public Optional<Adapter<?>> getAdapter( long id ) {
        return Optional.ofNullable( adapterById.get( id ) );
    }


    public ImmutableMap<String, Adapter<?>> getAdapters() {
        return ImmutableMap.copyOf( adapterByName );
    }


    @NotNull
    public Optional<DataStore<?>> getStore( String uniqueName ) {
        return getAdapter( uniqueName ).filter( a -> a instanceof DataStore<?> ).map( a -> (DataStore<?>) a );

    }


    @NotNull
    public Optional<DataStore<?>> getStore( long id ) {
        return getAdapter( id ).filter( a -> a instanceof DataStore<?> ).map( a -> (DataStore<?>) a );
    }


    public ImmutableMap<String, DataStore<?>> getStores() {
        Map<String, DataStore<?>> map = new HashMap<>();
        for ( Entry<String, Adapter<?>> entry : getAdapters().entrySet() ) {
            if ( entry.getValue() instanceof DataStore<?> ) {
                map.put( entry.getKey(), (DataStore<?>) entry.getValue() );
            }
        }
        return ImmutableMap.copyOf( map );
    }


    @NotNull
    public Optional<DataSource<?>> getSource( String uniqueName ) {
        return getAdapter( uniqueName ).filter( a -> a instanceof DataSource<?> ).map( a -> (DataSource<?>) a );
    }


    @NotNull
    public Optional<DataSource<?>> getSource( long id ) {
        return getAdapter( id ).filter( a -> a instanceof DataSource<?> ).map( a -> (DataSource<?>) a );
    }


    public ImmutableMap<String, DataSource<?>> getSources() {
        Map<String, DataSource<?>> map = new HashMap<>();
        for ( Entry<String, Adapter<?>> entry : getAdapters().entrySet() ) {
            if ( entry.getValue() instanceof DataSource<?> ) {
                map.put( entry.getKey(), (DataSource<?>) entry.getValue() );
            }
        }
        return ImmutableMap.copyOf( map );
    }


    public Adapter<?> addAdapter( String adapterName, String uniqueName, AdapterType adapterType, DeployMode mode, Map<String, String> settings ) {
        uniqueName = uniqueName.toLowerCase();
        if ( getAdapters().containsKey( uniqueName ) ) {
            throw new GenericRuntimeException( "There is already an adapter with this unique name" );
        }
        if ( !settings.containsKey( "mode" ) ) {
            throw new GenericRuntimeException( "The adapter does not specify a mode which is necessary." );
        }

        AdapterTemplate adapterTemplate = AdapterTemplate.fromString( adapterName, adapterType );

        long adapterId = Catalog.getInstance().createAdapter( uniqueName, adapterName, adapterType, settings, mode );
        try {
            Adapter<?> adapter = adapterTemplate.getDeployer().get( adapterId, uniqueName, settings );
            adapterByName.put( adapter.getUniqueName(), adapter );
            adapterById.put( adapter.getAdapterId(), adapter );
            return adapter;
        } catch ( Exception e ) {
            Catalog.getInstance().dropAdapter( adapterId );
            throw new GenericRuntimeException( "Something went wrong while adding a new adapter", e );
        }
    }


    public void removeAdapter( long adapterId ) {
        Optional<Adapter<?>> optionalAdapter = getAdapter( adapterId );
        if ( optionalAdapter.isEmpty() ) {
            throw new GenericRuntimeException( "Unknown adapter instance with id: %s", adapterId );
        }
        Adapter<?> adapterInstance = optionalAdapter.get();

        LogicalAdapter logicalAdapter = Catalog.getInstance().getSnapshot().getAdapter( adapterId ).orElseThrow();

        // Check if the store has any placements
        List<AllocationEntity> placements = Catalog.getInstance().getSnapshot().alloc().getEntitiesOnAdapter( logicalAdapter.id ).orElseThrow( () -> new GenericRuntimeException( "There is still data placed on this data store" ) );
        if ( !placements.isEmpty() ) {
            if ( adapterInstance instanceof DataStore<?> ) {
                throw new GenericRuntimeException( "There is still data placed on this data store" );
            }
        }

        // Shutdown store
        adapterInstance.shutdownAndRemoveListeners();

        // Remove store from maps
        adapterById.remove( adapterInstance.getAdapterId() );
        adapterByName.remove( adapterInstance.getUniqueName() );

        // Delete store from catalog
        Catalog.getInstance().dropAdapter( logicalAdapter.id );
    }


    /**
     * Restores adapters from catalog
     */
    public void restoreAdapters( List<LogicalAdapter> adapters ) {
        try {
            for ( LogicalAdapter adapter : adapters ) {
                Constructor<?> ctor = AdapterTemplate.fromString( adapter.adapterName, adapter.type ).getClazz().getConstructor( long.class, String.class, Map.class );
                Adapter<?> instance = (Adapter<?>) ctor.newInstance( adapter.id, adapter.uniqueName, adapter.settings );
                adapterByName.put( instance.getUniqueName(), instance );
                adapterById.put( instance.getAdapterId(), instance );
            }
        } catch ( NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e ) {
            throw new GenericRuntimeException( "Something went wrong while restoring adapters from the catalog.", e );
        }
    }


    public record AdapterInformation(String name, String description, AdapterType type, List<AbstractAdapterSetting> settings, List<DeployMode> modes) {

        public static JsonSerializer<AdapterInformation> getSerializer() {
            return ( src, typeOfSrc, context ) -> {
                JsonObject jsonStore = new JsonObject();
                jsonStore.addProperty( "name", src.name );
                jsonStore.addProperty( "description", src.description );
                jsonStore.addProperty( "type", src.type.name() );
                jsonStore.add( "adapterSettings", context.serialize( src.settings ) );
                return jsonStore;
            };
        }

    }


    @FunctionalInterface
    public interface Function4<P1, P2, P3, R> {

        R get( P1 p1, P2 p2, P3 p3 );

    }

}
