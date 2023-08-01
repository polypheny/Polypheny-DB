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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Util;

public class PIPlugin extends PolyPlugin {

    public PIPlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void start() {
        Map<String, String> settings = new HashMap<>();
        settings.put( "port", "20590" );
        settings.put( "requires heartbeat", "false" );
        settings.put( "heartbeat intervall", "0" );
        QueryInterfaceManager.addInterfaceType( "proto-interface", ProtoInterface.class, settings );
    }


    public void stop() {
        QueryInterfaceManager.removeInterfaceType( ProtoInterface.class );
    }


    @Slf4j
    @Extension
    public static class ProtoInterface extends QueryInterface implements PropertyChangeListener {

        public static final String INTERFACE_NAME = "Proto Interface";
        public static final String INTERFACE_DESCRIPTION = "proto-interface query interface supporting the PolySQL dialect.";
        public static final List<QueryInterfaceSetting> AVAILABLE_SETTINGS = ImmutableList.of(
                new QueryInterfaceSettingInteger( "port", false, true, false, 20590 ),
                new QueryInterfaceSettingBoolean( "requires heartbeat", false, true, false, false ),
                new QueryInterfaceSettingLong( "heartbeat interval", false, true, false, 300000L )
        );
        @Getter
        private final int port;
        @Getter
        private final boolean requiresHeartbeat;
        @Getter
        private final long heartbeatIntervall;
        @Getter
        private TransactionManager transactionManager;
        @Getter
        private Authenticator authenticator;
        @Getter
        private ClientManager clientManager;
        private PIServer protoInterfaceServer;


        public ProtoInterface( TransactionManager transactionManager, Authenticator authenticator, long queryInterfaceId, String uniqueName, Map<String, String> settings ) {
            super( transactionManager, authenticator, queryInterfaceId, uniqueName, settings, true, true );
            this.authenticator = authenticator;
            this.transactionManager = transactionManager;
            this.port = Integer.parseInt( settings.get( "port" ) );
            if ( !Util.checkIfPortIsAvailable( port ) ) {
                // Port is already in use
                throw new RuntimeException( "Unable to start " + INTERFACE_NAME + " on port " + port + "! The port is already in use." );
            }
            this.requiresHeartbeat = Boolean.getBoolean(settings.get("requires heartbeat"));
            this.heartbeatIntervall = Long.parseLong(settings.get("heartbeat intervall"));
        }


        @Override
        public List<QueryInterfaceSetting> getAvailableSettings() {
            return AVAILABLE_SETTINGS;
        }


        @Override
        public void shutdown() {
            try {
                protoInterfaceServer.shutdown();
            } catch ( InterruptedException e ) {
                throw new RuntimeException( e );
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
            protoInterfaceServer = new PIServer( this );
            // protoInterfaceServer = new PIServer( port, protoInterfaceService, clientManager );
            try {
                protoInterfaceServer.start();
            } catch ( IOException e ) {
                log.error( "Proto interface server could not be started: {}", e.getMessage() );
            }
        }

    }

}
