/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.protointerface;

import com.google.common.collect.ImmutableList;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.protointerface.PIPlugin.ProtoInterface.Transport;
import org.polypheny.db.transaction.TransactionManager;

public class PIPlugin extends PolyPlugin {

    public PIPlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void afterCatalogInit() {
        QueryInterfaceManager.addInterfaceTemplate( ProtoInterface.INTERFACE_NAME + " (Plain transport)",
                ProtoInterface.INTERFACE_DESCRIPTION, ProtoInterface.AVAILABLE_PLAIN_SETTINGS, ( a, b, c, d ) -> new ProtoInterface( a, b, c, Transport.PLAIN, d ) );
        QueryInterfaceManager.addInterfaceTemplate( ProtoInterface.INTERFACE_NAME + " (Unix transport)",
                ProtoInterface.INTERFACE_DESCRIPTION, ProtoInterface.AVAILABLE_UNIX_SETTINGS, ( a, b, c, d ) -> new ProtoInterface( a, b, c, Transport.UNIX, d ) );
    }


    public void stop() {
        QueryInterfaceManager.removeInterfaceType( ProtoInterface.INTERFACE_NAME );
    }


    @Slf4j
    @Extension
    public static class ProtoInterface extends QueryInterface implements PropertyChangeListener {

        public static final String INTERFACE_NAME = "Proto Interface";
        public static final String INTERFACE_DESCRIPTION = "proto-interface query interface supporting the PolySQL dialect.";
        public static final List<QueryInterfaceSetting> AVAILABLE_PLAIN_SETTINGS = ImmutableList.of(
                new QueryInterfaceSettingInteger( "port", false, true, false, 20590 ),
                new QueryInterfaceSettingBoolean( "requires heartbeat", false, true, false, false ),
                new QueryInterfaceSettingLong( "heartbeat interval", false, true, false, 300000L )
        );
        public static final List<QueryInterfaceSetting> AVAILABLE_UNIX_SETTINGS = ImmutableList.of(
                new QueryInterfaceSettingString( "path", false, true, false, "polypheny-proto.sock" )
        );

        @Getter
        private final boolean requiresHeartbeat;
        @Getter
        private final long heartbeatInterval;
        @Getter
        private final TransactionManager transactionManager;
        @Getter
        private final Authenticator authenticator;
        @Getter
        private ClientManager clientManager;
        private PIServer protoInterfaceServer;
        @Getter
        private MonitoringPage monitoringPage;


        enum Transport {
            PLAIN,
            UNIX,
        }


        private Transport transport;


        private ProtoInterface( TransactionManager transactionManager, Authenticator authenticator, String uniqueName, Transport transport, Map<String, String> settings ) {
            super( transactionManager, authenticator, uniqueName, settings, true, true );
            this.authenticator = authenticator;
            this.transactionManager = transactionManager;
            this.transport = transport;
            if ( getAvailableSettings().stream().anyMatch( s -> s.name.equals( "requires heartbeat" ) ) ) {
                this.requiresHeartbeat = Boolean.getBoolean( settings.get( "requires heartbeat" ) );
                this.heartbeatInterval = Long.parseLong( settings.get( "heartbeat interval" ) );
            } else {
                this.requiresHeartbeat = false;
                this.heartbeatInterval = 0;
            }
            this.monitoringPage = new MonitoringPage( uniqueName, INTERFACE_NAME );
        }


        @Override
        public List<QueryInterfaceSetting> getAvailableSettings() {
            return switch ( transport ) {
                case PLAIN -> AVAILABLE_PLAIN_SETTINGS;
                case UNIX -> AVAILABLE_UNIX_SETTINGS;
            };
        }


        @Override
        public void shutdown() {
            try {
                protoInterfaceServer.shutdown();
            } catch ( IOException | InterruptedException e ) {
                throw new GenericRuntimeException( e );
            }
        }


        @Override
        public String getInterfaceType() {
            return INTERFACE_NAME;
        }


        @Override
        protected void reloadSettings( List<String> updatedSettings ) {
        }


        @Override
        public void propertyChange( PropertyChangeEvent evt ) {

        }


        @Override
        public void languageChange() {

        }


        @Override
        public void run() {
            clientManager = new ClientManager( this );
            try {
                protoInterfaceServer = PIServer.startServer( clientManager, transport, settings );
            } catch ( IOException e ) {
                log.error( "Proto interface server could not be started: {}", e.getMessage() );
                throw new GenericRuntimeException( e );
            }
        }

    }

}
