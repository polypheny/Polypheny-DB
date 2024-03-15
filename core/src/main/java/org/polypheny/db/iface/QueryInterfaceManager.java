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


    public static void addInterfaceTemplate( String interfaceName, String description,
            List<QueryInterfaceSetting> availableSettings, Function6<TransactionManager, Authenticator, Long, String, Map<String, String>, QueryInterface> deployer ) {
        Catalog.getInstance().createInterfaceTemplate( interfaceName, new QueryInterfaceTemplate( interfaceName, description,
                deployer, availableSettings ) );
    }


    public static void removeInterfaceType( String interfaceName ) {

        for ( LogicalQueryInterface queryInterface : Catalog.getInstance().getSnapshot().getQueryInterfaces() ) {
            if ( queryInterface.interfaceName.equals( interfaceName ) ) {
                throw new GenericRuntimeException( "Cannot remove the interface type, there is still a interface active." );
            }
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


    private void startInterface( QueryInterface instance ) {
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


    /**
     * Restores query interfaces from catalog
     */
    public void restoreInterfaces( Snapshot snapshot ) {
        List<LogicalQueryInterface> interfaces = snapshot.getQueryInterfaces();
        for ( LogicalQueryInterface iface : interfaces ) {
            QueryInterfaceTemplate template = Catalog.snapshot().getInterfaceTemplate( iface.getInterfaceName() ).orElseThrow();
            QueryInterface instance = template.deployer().get( transactionManager, authenticator, iface.id, iface.name, iface.settings );
            startInterface( instance );
        }
    }


    public QueryInterface createQueryInterface( String interfaceName, String uniqueName, Map<String, String> settings ) {
        uniqueName = uniqueName.toLowerCase();
        if ( interfaceByName.containsKey( uniqueName ) ) {
            throw new GenericRuntimeException( "There is already a query interface with this unique name" );
        }
        QueryInterface instance;
        QueryInterfaceTemplate template = Catalog.snapshot().getInterfaceTemplate( interfaceName ).orElseThrow();
        long ifaceId = Catalog.getInstance().createQueryInterface( uniqueName, interfaceName, settings );
        instance = template.deployer().get( transactionManager, authenticator, ifaceId, uniqueName, settings );
        startInterface( instance );

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
            Function6<TransactionManager, Authenticator, Long, String, Map<String, String>, QueryInterface> deployer,
            @JsonSerialize List<QueryInterfaceSetting> availableSettings) {

        public Map<String, String> getDefaultSettings() {
            Map<String, String> m = new HashMap<>();
            availableSettings.forEach( ( s ) -> m.put( s.name, s.getDefault() ) );
            return m;
        }

    }


    @FunctionalInterface
    public interface Function6<P1, P2, P3, P4, P5, R> {

        R get( P1 p1, P2 p2, P3 p3, P4 p4, P5 p5 );

    }

}
