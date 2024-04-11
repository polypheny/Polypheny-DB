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

package org.polypheny.db.iface;


import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalQueryInterface;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.iface.QueryInterface.QueryInterfaceSetting;
import org.polypheny.db.transaction.TransactionManager;


@Slf4j
public class QueryInterfaceManager {

    private static QueryInterfaceManager INSTANCE;

    private final Map<Long, QueryInterface> interfaceById = new HashMap<>();
    private final Map<String, QueryInterface> interfaceByName = new HashMap<>();
    private final Map<Long, Thread> interfaceThreadById = new HashMap<>();

    private final TransactionManager transactionManager;
    private final Authenticator authenticator;


    public static QueryInterfaceManager getInstance() {
        if ( INSTANCE == null ) {
            throw new GenericRuntimeException( "Interface manager has not yet been initialized" );
        }
        return INSTANCE;
    }


    public static void initialize( TransactionManager transactionManager, Authenticator authenticator ) {
        INSTANCE = new QueryInterfaceManager( transactionManager, authenticator );
    }


    private QueryInterfaceManager( TransactionManager transactionManager, Authenticator authenticator ) {
        this.transactionManager = transactionManager;
        this.authenticator = authenticator;
    }


    public QueryInterface getQueryInterface( String uniqueName ) {
        return interfaceByName.get( uniqueName.toLowerCase() );
    }


    public static void addInterfaceType( String interfaceName, Class<? extends QueryInterface> clazz, Map<String, String> defaultSettings ) {
        Catalog.getInstance().createInterfaceTemplate( clazz.getSimpleName(), new QueryInterfaceTemplate( clazz, interfaceName, defaultSettings ) );
        //REGISTER.put( clazz.getSimpleName(), new QueryInterfaceType( clazz, interfaceName, defaultSettings ) );
    }


    public static void removeInterfaceType( Class<? extends QueryInterface> clazz ) {
        for ( LogicalQueryInterface queryInterface : Catalog.getInstance().getSnapshot().getQueryInterfaces() ) {
            if ( queryInterface.clazz.equals( clazz.getName() ) ) {
                throw new GenericRuntimeException( "Cannot remove the interface type, there is still a interface active." );
            }
        }
        Catalog.getInstance().dropInterfaceTemplate( clazz.getSimpleName() );
    }


    public QueryInterface getQueryInterface( int id ) {
        return interfaceById.get( id );
    }


    public ImmutableMap<String, QueryInterface> getQueryInterfaces() {
        return ImmutableMap.copyOf( interfaceByName );
    }


    public List<QueryInterfaceInformation> getAvailableQueryInterfaceTypes() {
        List<QueryInterfaceInformation> result = new LinkedList<>();
        try {
            for ( Class<? extends QueryInterface> clazz : Catalog.snapshot().getInterfaceTemplates().stream().map( v -> v.clazz ).collect( Collectors.toList() ) ) {
                // Exclude abstract classes
                if ( !Modifier.isAbstract( clazz.getModifiers() ) ) {
                    String name = (String) clazz.getDeclaredField( "INTERFACE_NAME" ).get( null );
                    String description = (String) clazz.getDeclaredField( "INTERFACE_DESCRIPTION" ).get( null );
                    List<QueryInterfaceSetting> settings = (List<QueryInterfaceSetting>) clazz.getDeclaredField( "AVAILABLE_SETTINGS" ).get( null );
                    result.add( new QueryInterfaceInformation( name, description, clazz, settings ) );
                }
            }
        } catch ( NoSuchFieldException | IllegalAccessException e ) {
            throw new GenericRuntimeException( "Something went wrong while retrieving list of available query interface types.", e );
        }
        return result;
    }


    /**
     * Restores query interfaces from catalog
     */
    public void restoreInterfaces( Snapshot snapshot ) {
        try {
            List<LogicalQueryInterface> interfaces = snapshot.getQueryInterfaces();
            for ( LogicalQueryInterface iface : interfaces ) {
                String[] split = iface.clazz.split( "\\$" );
                split = split[split.length - 1].split( "\\." );
                Class<?> clazz = Catalog.snapshot().getInterfaceTemplate( split[split.length - 1] ).orElseThrow().clazz;
                Constructor<?> ctor = clazz.getConstructor( TransactionManager.class, Authenticator.class, long.class, String.class, Map.class );
                QueryInterface instance = (QueryInterface) ctor.newInstance( transactionManager, authenticator, iface.id, iface.name, iface.settings );

                Thread thread = new Thread( instance );
                thread.start();

                try {
                    thread.join();
                } catch ( InterruptedException e ) {
                    log.warn( "Interrupted on join()", e );
                }

                interfaceByName.put( instance.getUniqueName(), instance );
                interfaceById.put( instance.getQueryInterfaceId(), instance );
                interfaceThreadById.put( instance.getQueryInterfaceId(), thread );
            }
        } catch ( NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e ) {
            throw new GenericRuntimeException( "Something went wrong while restoring query interfaces from the catalog.", e );
        }
    }


    public QueryInterface addQueryInterface( Catalog catalog, String clazzName, String uniqueName, Map<String, String> settings ) {
        uniqueName = uniqueName.toLowerCase();
        if ( interfaceByName.containsKey( uniqueName ) ) {
            throw new GenericRuntimeException( "There is already a query interface with this unique name" );
        }
        QueryInterface instance;
        long ifaceId = -1;
        try {
            String[] split = clazzName.split( "\\$" );
            split = split[split.length - 1].split( "\\." );
            Class<?> clazz = Catalog.snapshot().getInterfaceTemplate( split[split.length - 1] ).orElseThrow().clazz;
            Constructor<?> ctor = clazz.getConstructor( TransactionManager.class, Authenticator.class, long.class, String.class, Map.class );
            ifaceId = catalog.createQueryInterface( uniqueName, clazzName, settings );
            instance = (QueryInterface) ctor.newInstance( transactionManager, authenticator, ifaceId, uniqueName, settings );

            Thread thread = new Thread( instance );
            thread.start();

            try {
                thread.join();
            } catch ( InterruptedException e ) {
                log.warn( "Interrupted on join()", e );
            }

            interfaceByName.put( instance.getUniqueName(), instance );
            interfaceById.put( instance.getQueryInterfaceId(), instance );
            interfaceThreadById.put( instance.getQueryInterfaceId(), thread );
        } catch ( InvocationTargetException e ) {
            if ( ifaceId != -1 ) {
                catalog.dropQueryInterface( ifaceId );
            }
            throw new GenericRuntimeException( "Something went wrong while adding a new query interface: " + e.getCause().getMessage(), e );
        } catch ( NoSuchMethodException | IllegalAccessException | InstantiationException e ) {
            if ( ifaceId != -1 ) {
                catalog.dropQueryInterface( ifaceId );
            }
            throw new GenericRuntimeException( "Something went wrong while adding a new query interface!", e );
        }
        return instance;
    }


    public void removeQueryInterface( Catalog catalog, String uniqueName ) {
        uniqueName = uniqueName.toLowerCase();
        if ( !interfaceByName.containsKey( uniqueName ) ) {
            throw new GenericRuntimeException( "Unknown query interface: " + uniqueName );
        }
        LogicalQueryInterface logicalQueryInterface = catalog.getSnapshot().getQueryInterface( uniqueName ).orElseThrow();

        // Shutdown interface
        interfaceByName.get( uniqueName ).shutdown();

        // Remove interfaces from maps
        interfaceById.remove( logicalQueryInterface.id );
        interfaceByName.remove( uniqueName );
        interfaceThreadById.remove( logicalQueryInterface.id );

        // Delete query interface from catalog
        catalog.dropQueryInterface( logicalQueryInterface.id );
    }


    @AllArgsConstructor
    public static class QueryInterfaceInformation {

        public final String name;
        public final String description;
        public final Class<?> clazz;
        public final List<QueryInterfaceSetting> availableSettings;


        public static String toJson( QueryInterfaceInformation[] queryInterfaceInformations ) {
            JsonSerializer<QueryInterfaceInformation> queryInterfaceInformationSerializer = ( src, typeOfSrc, context ) -> {
                JsonObject jsonStore = new JsonObject();
                jsonStore.addProperty( "name", src.name );
                jsonStore.addProperty( "description", src.description );
                jsonStore.addProperty( "clazz", src.clazz.getCanonicalName() );
                jsonStore.add( "availableSettings", context.serialize( src.availableSettings ) );
                return jsonStore;
            };
            Gson qiiGson = new GsonBuilder().registerTypeAdapter( QueryInterfaceInformation.class, queryInterfaceInformationSerializer ).create();
            return qiiGson.toJson( queryInterfaceInformations, QueryInterfaceInformation[].class );
        }

    }


    /**
     * Model needed for the UI
     */
    public static class QueryInterfaceInformationRequest {

        public String clazzName;
        public String uniqueName;
        public Map<String, String> currentSettings;

    }


    @AllArgsConstructor
    public static class QueryInterfaceTemplate {

        public Class<? extends QueryInterface> clazz;
        public String interfaceName;
        public Map<String, String> defaultSettings;

    }

}
