package org.polypheny.db.adapter;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.polypheny.db.adapter.Adapter.AdapterSetting;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;

public abstract class AdapterManager {

    private final Map<Integer, Adapter> adapterById = new HashMap<>();
    private final Map<String, Adapter> adapterByName = new HashMap<>();


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


    // Called by StoreManager.addStore() and DataSourceManager.addSource()
    protected void addAdapter( Adapter instance ) {
        adapterByName.put( instance.getUniqueName(), instance );
        adapterById.put( instance.getAdapterId(), instance );
    }


    // Called by StoreManager.removeStore() and DataSourceManager.removeSource()
    protected void removeAdapter( Adapter instance ) {
        adapterById.remove( instance.getAdapterId() );
        adapterByName.remove( instance.getUniqueName() );
    }


    /**
     * Restores adapters from catalog
     */
    public void restoreAdapters( Catalog catalog ) {
        try {
            List<CatalogAdapter> adapters = catalog.getAdapters();
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
        public final List<AdapterSetting> settings;

    }

}
