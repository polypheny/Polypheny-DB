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


import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
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


    public static void addInterfaceTemplate(
            String interfaceName, String description,
            List<QueryInterfaceSetting> availableSettings, Function5<TransactionManager, Authenticator, String, Map<String, String>, QueryInterface> deployer ) {
        Catalog.getInstance().createInterfaceTemplate( interfaceName, new QueryInterfaceTemplate( interfaceName, description,
                deployer, availableSettings ) );
    }


    public static void removeInterfaceType( String interfaceName ) {
        if ( Catalog.snapshot().getQueryInterfaces().values().stream().anyMatch( i -> i.getInterfaceName().equals( interfaceName ) ) ) {
            throw new GenericRuntimeException( "Cannot remove the interface type, there is still a interface active." );
        }
        Catalog.getInstance().dropInterfaceTemplate( interfaceName );
    }


    public QueryInterface getQueryInterface( int id ) {
        return interfaceById.get( id );
    }


    public ImmutableMap<String, QueryInterface> getQueryInterfaces() {
        return ImmutableMap.copyOf( interfaceByName );
    }


    public List<QueryInterfaceTemplate> getAvailableQueryInterfaceTemplates() {
        return Catalog.snapshot().getInterfaceTemplates();
    }


    private void startInterface( QueryInterface instance, String interfaceName, Long id ) {
        Thread thread = new Thread( instance );
        AtomicReference<Throwable> error = new AtomicReference<>();
        thread.setUncaughtExceptionHandler( ( Thread t, Throwable e ) -> error.set( e ) );
        thread.start();

        try {
            thread.join();
        } catch ( InterruptedException e ) {
            log.warn( "Interrupted on join()", e );
        }
        if ( error.get() != null ) {
            throw new GenericRuntimeException( error.get() );
        }

        if ( id == null ) {
            id = Catalog.getInstance().createQueryInterface( instance.getUniqueName(), interfaceName, instance.getCurrentSettings() );
        }
        interfaceByName.put( instance.getUniqueName(), instance );
        interfaceById.put( id, instance );
        interfaceThreadById.put( id, thread );
    }


    /**
     * Restores query interfaces from catalog
     */
    public void restoreInterfaces( Snapshot snapshot ) {
        Map<Long, LogicalQueryInterface> interfaces = snapshot.getQueryInterfaces();
        interfaces.forEach( ( id, l ) -> {
                    QueryInterface q = Catalog.snapshot().getInterfaceTemplate( l.interfaceName )
                            .map( t -> t.deployer.get( transactionManager, authenticator, l.name, l.settings ) )
                            .orElseThrow();
                    startInterface( q, l.interfaceName, id );
                }
        );
    }


    public QueryInterface createQueryInterface( String interfaceName, String uniqueName, Map<String, String> settings ) {
        uniqueName = uniqueName.toLowerCase();
        if ( interfaceByName.containsKey( uniqueName ) ) {
            throw new GenericRuntimeException( "There is already a query interface with this unique name" );
        }
        QueryInterface instance;
        QueryInterfaceTemplate template = Catalog.snapshot().getInterfaceTemplate( interfaceName ).orElseThrow();
        try {
            instance = template.deployer().get( transactionManager, authenticator, uniqueName, settings );
        } catch ( GenericRuntimeException e ) {
            throw new GenericRuntimeException( "Failed to deploy query interface: " + e.getMessage() );
        }
        startInterface( instance, interfaceName, null );

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


    /**
     * Model needed for the UI
     */
    public record QueryInterfaceInformationRequest(
            @JsonSerialize String interfaceName,
            @JsonSerialize String uniqueName,
            @JsonSerialize Map<String, String> currentSettings) {

    }


    public record QueryInterfaceTemplate(
            @JsonSerialize String interfaceName,
            @JsonSerialize String description,
            Function5<TransactionManager, Authenticator, String, Map<String, String>, QueryInterface> deployer,
            @JsonSerialize List<QueryInterfaceSetting> availableSettings) {

        public Map<String, String> getDefaultSettings() {
            Map<String, String> m = new HashMap<>();
            availableSettings.forEach( ( s ) -> m.put( s.name, s.getDefault() ) );
            return m;
        }

    }


    @FunctionalInterface
    public interface Function5<P1, P2, P3, P4, R> {

        R get( P1 p1, P2 p2, P3 p3, P4 p4 );

    }

}
