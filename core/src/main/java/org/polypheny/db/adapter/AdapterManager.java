package org.polypheny.db.adapter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.polypheny.db.adapter.Adapter.AdapterSetting;
import org.polypheny.db.adapter.Adapter.AdapterSettingList;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.reflections.Reflections;

public class AdapterManager {

    private final Map<Integer, Adapter> adapterById = new HashMap<>();
    private final Map<String, Adapter> adapterByName = new HashMap<>();


    private static final AdapterManager INSTANCE = new AdapterManager();


    public static AdapterManager getInstance() {
        return INSTANCE;
    }


    private AdapterManager() {
        // intentionally empty
    }


    public Adapter getAdapter( String uniqueName ) {
        uniqueName = uniqueName.toLowerCase();
        return adapterByName.get( uniqueName );
    }


    public Adapter getAdapter( int id ) {
        return adapterById.get( id );
    }


    public ImmutableMap<String, Adapter> getAdapters() {
        return ImmutableMap.copyOf( adapterByName );
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


    public DataSource getSource( String uniqueName ) {
        Adapter adapter = getAdapter( uniqueName );
        if ( adapter instanceof DataSource ) {
            return (DataSource) adapter;
        }
        return null;
    }


    public DataSource getSource( int id ) {
        Adapter adapter = getAdapter( id );
        if ( adapter instanceof DataSource ) {
            return (DataSource) adapter;
        }
        return null;
    }


    public ImmutableMap<String, DataSource> getSources() {
        Map<String, DataSource> map = new HashMap<>();
        for ( Entry<String, Adapter> entry : getAdapters().entrySet() ) {
            if ( entry.getValue() instanceof DataSource ) {
                map.put( entry.getKey(), (DataSource) entry.getValue() );
            }
        }
        return ImmutableMap.copyOf( map );
    }


    public List<AdapterInformation> getAvailableAdapters( AdapterType adapterType ) {
        Reflections reflections = new Reflections( "org.polypheny.db" );
        Set<Class> classes;
        if ( adapterType == AdapterType.STORE ) {
            classes = ImmutableSet.copyOf( reflections.getSubTypesOf( DataStore.class ) );
        } else if ( adapterType == AdapterType.SOURCE ) {
            classes = ImmutableSet.copyOf( reflections.getSubTypesOf( DataSource.class ) );
        } else {
            throw new RuntimeException( "Unknown adapter type: " + adapterType );
        }
        List<AdapterInformation> result = new LinkedList<>();
        try {
            //noinspection unchecked
            for ( Class<DataStore> clazz : classes ) {
                // Exclude abstract classes
                if ( !Modifier.isAbstract( clazz.getModifiers() ) ) {
                    String name = (String) clazz.getDeclaredField( "ADAPTER_NAME" ).get( null );
                    String description = (String) clazz.getDeclaredField( "DESCRIPTION" ).get( null );
                    Map<String, List<AdapterSetting>> settings = new HashMap<>();
                    settings.put( "default", (List<AdapterSetting>) clazz.getDeclaredField( "AVAILABLE_SETTINGS" ).get( null ) );
                    if ( Arrays.asList( clazz.getGenericInterfaces() ).contains( DockerDeployable.class ) ) {
                        settings.put( "docker", (List<AdapterSetting>) clazz.getField( "AVAILABLE_DOCKER_SETTINGS" ).get( null ) );
                    }
                    if ( Arrays.asList( clazz.getGenericInterfaces() ).contains( RemoteDeployable.class ) ) {
                        settings.put( "remote", (List<AdapterSetting>) clazz.getDeclaredField( "AVAILABLE_REMOTE_SETTINGS" ).get( null ) );
                    }
                    settings.put( "mode", Collections.singletonList( new AdapterSettingList( "mode", false, true, true, Collections.singletonList( "docker" ) ) ) );
                    result.add( new AdapterInformation( name, description, clazz, settings ) );
                }
            }
        } catch ( NoSuchFieldException | IllegalAccessException e ) {
            throw new RuntimeException( "Something went wrong while retrieving list of available store adapters.", e );
        }
        return result;
    }


    public Adapter addAdapter( String clazzName, String uniqueName, Map<String, String> settings ) {
        uniqueName = uniqueName.toLowerCase();
        if ( getAdapters().containsKey( uniqueName ) ) {
            throw new RuntimeException( "There is already an adapter with this unique name" );
        }
        // for clarity we add the default mode if none is specified
        if ( !settings.containsKey( "mode" ) ) {
            settings.put( "mode", "default" );
        }

        Constructor<?> ctor;
        AdapterType adapterType;
        try {
            Class<?> clazz = Class.forName( clazzName );
            ctor = clazz.getConstructor( int.class, String.class, Map.class );

            // Determine adapter type
            if ( DataStore.class.isAssignableFrom( clazz ) ) {
                adapterType = AdapterType.STORE;
            } else if ( DataSource.class.isAssignableFrom( clazz ) ) {
                adapterType = AdapterType.SOURCE;
            } else {
                throw new RuntimeException( "Unknown type of adapter! Specified class is neither implementing DataStore nor DataSource." );
            }
        } catch ( NoSuchMethodException | ClassNotFoundException e ) {
            throw new RuntimeException( "Something went wrong while adding a new adapter", e );
        }

        int adapterId = Catalog.getInstance().addAdapter( uniqueName, clazzName, adapterType, settings );
        Adapter instance;
        try {
            instance = (Adapter) ctor.newInstance( adapterId, uniqueName, settings );
        } catch ( Exception e ) {
            Catalog.getInstance().deleteAdapter( adapterId );
            if ( e instanceof InvocationTargetException ) {
                Throwable t = ((InvocationTargetException) e).getTargetException();
                if ( t instanceof RuntimeException ) {
                    throw (RuntimeException) t;
                }
            } else {
                Catalog.getInstance().deleteAdapter( adapterId );
            }
            throw new RuntimeException( "Something went wrong while adding a new adapter", e );
        }
        adapterByName.put( instance.getUniqueName(), instance );
        adapterById.put( instance.getAdapterId(), instance );

        return instance;
    }


    public void removeAdapter( int adapterId ) {
        Adapter adapterInstance = getAdapter( adapterId );
        if ( adapterInstance == null ) {
            throw new RuntimeException( "Unknown adapter instance with id: " + adapterId );
        }
        CatalogAdapter catalogAdapter = Catalog.getInstance().getAdapter( adapterId );

        // Check if the store has any placements
        List<CatalogColumnPlacement> placements = Catalog.getInstance().getColumnPlacementsOnAdapter( catalogAdapter.id );
        if ( placements.size() != 0 ) {
            throw new RuntimeException( "There is still data placed on this data store" );
        }

        // Remove used listener
        if ( adapterInstance instanceof DockerDeployable ) {
            adapterInstance.removeListener();
        }

        // Shutdown store
        adapterInstance.shutdown();

        // Remove store from maps
        adapterById.remove( adapterInstance.getAdapterId() );
        adapterByName.remove( adapterInstance.getUniqueName() );

        // Delete store from catalog
        Catalog.getInstance().deleteAdapter( catalogAdapter.id );
    }


    /**
     * Restores adapters from catalog
     */
    public void restoreAdapters() {
        try {
            List<CatalogAdapter> adapters = Catalog.getInstance().getAdapters();
            for ( CatalogAdapter adapter : adapters ) {
                Class<?> clazz = Class.forName( adapter.adapterClazz );
                Constructor<?> ctor = clazz.getConstructor( int.class, String.class, Map.class );
                Adapter instance = (Adapter) ctor.newInstance( adapter.id, adapter.uniqueName, adapter.settings );
                adapterByName.put( instance.getUniqueName(), instance );
                adapterById.put( instance.getAdapterId(), instance );
            }
        } catch ( NoSuchMethodException | ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException e ) {
            throw new RuntimeException( "Something went wrong while restoring adapters from the catalog.", e );
        }
    }


    @AllArgsConstructor
    public static class AdapterInformation {

        public final String name;
        public final String description;
        public final Class clazz;
        public final Map<String, List<AdapterSetting>> settings;

    }

}
