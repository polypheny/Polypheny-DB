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

package org.polypheny.db.avatica;


import com.google.common.collect.ImmutableList;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.MetricsSystemConfiguration;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystemConfiguration;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.server.AvaticaHandler;
import org.apache.calcite.avatica.server.HandlerFactory;
import org.pf4j.Extension;
import org.polypheny.db.StatusNotificationService;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Util;


@SuppressWarnings("unused")
public class AvaticaInterfacePlugin extends PolyPlugin {

    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public AvaticaInterfacePlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void afterCatalogInit() {
        // Add JDBC interface
        Map<String, String> settings = new HashMap<>();
        settings.put( "port", "20591" );
        settings.put( "serialization", "PROTOBUF" );
        QueryInterfaceManager.addInterfaceType( "avatica", AvaticaInterface.class, settings );
    }


    @Override
    public void stop() {
        QueryInterfaceManager.removeInterfaceType( AvaticaInterface.class );
    }


    @Slf4j
    @Extension
    public static class AvaticaInterface extends QueryInterface implements PropertyChangeListener {

        @SuppressWarnings("WeakerAccess")
        public static final String INTERFACE_NAME = "AVATICA Interface";
        @SuppressWarnings({ "WeakerAccess", "unused" })
        public static final String INTERFACE_DESCRIPTION = "AVATICA-SQL query interface supporting the Polypheny SQL dialect.";
        @SuppressWarnings("WeakerAccess")
        public static final List<QueryInterfaceSetting> AVAILABLE_SETTINGS = ImmutableList.of(
                new QueryInterfaceSettingInteger( "port", false, true, false, 20591 ),
                new QueryInterfaceSettingList( "serialization", false, true, false, ImmutableList.of( "PROTOBUF", "JSON" ) )
        );


        private final MetricsSystemConfiguration<?> metricsSystemConfiguration;
        private final MetricsSystem metricsSystem;
        private final int port;

        private final DbmsMeta meta;
        private final HttpServerDispatcher httpServerDispatcher;


        public AvaticaInterface( TransactionManager transactionManager, Authenticator authenticator, long ifaceId, String uniqueName, Map<String, String> settings ) {
            super( transactionManager, authenticator, ifaceId, uniqueName, settings, true, true );
            metricsSystemConfiguration = NoopMetricsSystemConfiguration.getInstance();
            metricsSystem = NoopMetricsSystem.getInstance();

            port = Integer.parseInt( settings.get( "port" ) );
            if ( !Util.checkIfPortIsAvailable( port ) ) {
                // Port is already in use
                throw new GenericRuntimeException( "Unable to start " + INTERFACE_NAME + " on port " + port + "! The port is already in use." );
            }

            meta = new DbmsMeta( transactionManager, authenticator, uniqueName );
            Serialization serialization = Serialization.valueOf( settings.get( "serialization" ) );
            AvaticaHandler handler = new HandlerFactory().getHandler(
                    new DbmsService( meta, metricsSystem ),
                    serialization,
                    metricsSystemConfiguration );
            try {
                httpServerDispatcher = new HttpServerDispatcher( port, handler );
            } catch ( Exception e ) {
                throw new GenericRuntimeException( "Exception while starting " + INTERFACE_NAME, e );
            }

        }


        @Override
        public void propertyChange( PropertyChangeEvent evt ) {

        }


        @Override
        public void languageChange() {

        }


        @Override
        public void run() {
            try {
                httpServerDispatcher.start();
            } catch ( Exception e ) {
                log.error( "Exception while starting " + INTERFACE_NAME, e );
            }

            StatusNotificationService.printInfo( String.format( "%s started and is listening on port %d.", INTERFACE_NAME, port ) );
        }


        @Override
        public List<QueryInterfaceSetting> getAvailableSettings() {
            return AVAILABLE_SETTINGS;
        }


        @Override
        public void shutdown() {
            try {
                httpServerDispatcher.stop();
                meta.shutdown();
            } catch ( Exception e ) {
                log.error( "Exception during shutdown of an " + INTERFACE_NAME + "!", e );
            }
            log.info( "{} stopped.", INTERFACE_NAME );
        }


        @Override
        protected void reloadSettings( List<String> updatedSettings ) {
            // There is no modifiable setting for this query interface
        }


        @Override
        public String getInterfaceType() {
            return INTERFACE_NAME;
        }


    }

}
