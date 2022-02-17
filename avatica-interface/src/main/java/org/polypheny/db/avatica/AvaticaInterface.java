/*
 * Copyright 2019-2022 The Polypheny Project
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
import org.polypheny.db.StatusService;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Util;


@Slf4j
public class AvaticaInterface extends QueryInterface {

    @SuppressWarnings("WeakerAccess")
    public static final String INTERFACE_NAME = "AVATICA Interface";
    @SuppressWarnings("WeakerAccess")
    public static final String INTERFACE_DESCRIPTION = "AVATICA-SQL query interface supporting the PolySQL dialect.";
    @SuppressWarnings("WeakerAccess")
    public static final List<QueryInterfaceSetting> AVAILABLE_SETTINGS = ImmutableList.of(
            new QueryInterfaceSettingInteger( "port", false, true, false, 20591 ),
            new QueryInterfaceSettingList( "serialization", false, true, false, ImmutableList.of( "PROTOBUF", "JSON" ) )
    );


    private final MetricsSystemConfiguration metricsSystemConfiguration;
    private final MetricsSystem metricsSystem;
    private final int port;

    private final DbmsMeta meta;
    private final HttpServerDispatcher httpServerDispatcher;


    public AvaticaInterface( TransactionManager transactionManager, Authenticator authenticator, int ifaceId, String uniqueName, Map<String, String> settings ) {
        super( transactionManager, authenticator, ifaceId, uniqueName, settings, true, true );
        metricsSystemConfiguration = NoopMetricsSystemConfiguration.getInstance();
        metricsSystem = NoopMetricsSystem.getInstance();

        port = Integer.parseInt( settings.get( "port" ) );
        if ( !Util.checkIfPortIsAvailable( port ) ) {
            // Port is already in use
            throw new RuntimeException( "Unable to start " + INTERFACE_NAME + " on port " + port + "! The port is already in use." );
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
            throw new RuntimeException( "Exception while starting " + INTERFACE_NAME, e );
        }
    }


    @Override
    public void run() {
        try {
            httpServerDispatcher.start();
        } catch ( Exception e ) {
            log.error( "Exception while starting " + INTERFACE_NAME, e );
        }

        StatusService.printInfo( String.format( "%s started and is listening on port %d.", INTERFACE_NAME, port ) );
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
