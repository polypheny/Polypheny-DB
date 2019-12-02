package ch.unibas.dmi.dbis.polyphenydb;


import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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


    public void register( final Store instance ) {
        storesByName.put( instance.getUniqueName(), instance );
        storesById.put( instance.getStoreId(), instance );
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
                String description = (String) clazz.getDeclaredField( "DESCRIPTION" ).get(null);
                result.add( new AdapterInformation(name, description, clazz) );
            }
        } catch ( NoSuchFieldException | IllegalAccessException e ) {
            e.printStackTrace();
        }
        return result;
    }


    @AllArgsConstructor
    public static class AdapterInformation {
        public final String name;
        public final String description;
        public final Class clazz;
    }

}
