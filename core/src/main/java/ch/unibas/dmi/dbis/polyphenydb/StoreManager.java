package ch.unibas.dmi.dbis.polyphenydb;


import ch.unibas.dmi.dbis.polyphenydb.Store.AdapterSetting;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDataPlacement;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogStore;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AllArgsConstructor;
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
        return storesByName.get( uniqueName );
    }


    public Store getStore( int id ) {
        return storesById.get( id );
    }


    public ImmutableMap<String, Store> getStores() {
        return ImmutableMap.copyOf( storesByName );
    }


    public List<AdapterInformation> getAvailableAdapters() {
        Reflections reflections = new Reflections( "ch.unibas.dmi.dbis.polyphenydb" );
        Set<Class> classes = ImmutableSet.copyOf( reflections.getSubTypesOf( Store.class ) );
        List<AdapterInformation> result = new LinkedList<>();
        try {
            //noinspection unchecked
            for ( Class<Store> clazz : classes ) {
                String name = (String) clazz.getDeclaredField( "ADAPTER_NAME" ).get(null);
                String description = (String) clazz.getDeclaredField( "DESCRIPTION" ).get( null );
                List<AdapterSetting> settings = (List<AdapterSetting>) clazz.getDeclaredField( "AVAILABLE_SETTINGS" ).get( null );
                result.add( new AdapterInformation( name, description, clazz, settings ) );
            }
        } catch ( NoSuchFieldException | IllegalAccessException e ) {
            e.printStackTrace();
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
        if (!storesByName.containsKey( uniqueName )) {
            throw new RuntimeException( "Unknown store: " + uniqueName );
        }
        try {
            CatalogStore catalogStore = catalog.getStore( uniqueName );

            // Check if the store has any placements
            List<CatalogDataPlacement> placements = catalog.getDataPlacementsByStore( catalogStore.id );
            if (placements.size() != 0) {
                throw new RuntimeException( "There is still data placed on this store" );
            }

            // Shutdown store
            storesByName.get( uniqueName ).shutdown();

            // delete store from catalog
            catalog.deleteStore( catalogStore.id );
        } catch ( GenericCatalogException e ) {
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
