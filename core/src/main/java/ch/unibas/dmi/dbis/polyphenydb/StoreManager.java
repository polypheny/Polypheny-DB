package ch.unibas.dmi.dbis.polyphenydb;


import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;


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


    public void register( int storeId, final String uniqueName, final Store instance ) {
        storesByName.put( uniqueName, instance );
        storesById.put( storeId, instance );
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
}
