package ch.unibas.dmi.dbis.polyphenydb;


import java.util.HashMap;
import java.util.Map;


public class StoreManager {

    private static final StoreManager INSTANCE = new StoreManager();

    private final Map<String, Store> stores = new HashMap<>();


    public static StoreManager getInstance() {
        return INSTANCE;
    }


    private StoreManager() {
        // intentionally empty
    }


    public String register( final String proposedUniqueName, final Store instance ) {
        String uniqueName = proposedUniqueName;
        if ( stores.containsKey( uniqueName ) ) {
            // There is already a store with this unique name registered
            int counter = 0;
            uniqueName = proposedUniqueName + counter;
            while ( stores.containsKey( uniqueName ) ) {
                counter++;
                uniqueName = proposedUniqueName + counter;
            }
        }
        stores.put( uniqueName, instance );
        return uniqueName;
    }


    public Map<String, Store> getStores() {
        return stores;
    }
}
