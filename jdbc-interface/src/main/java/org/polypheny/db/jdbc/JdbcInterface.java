/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.jdbc;


import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.MetricsSystemConfiguration;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystemConfiguration;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.server.AvaticaHandler;
import org.apache.calcite.avatica.server.HandlerFactory;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.transaction.TransactionManager;


@Slf4j
public class JdbcInterface extends QueryInterface {


    private final MetricsSystemConfiguration metricsSystemConfiguration;
    private final MetricsSystem metricsSystem;


    public JdbcInterface( TransactionManager transactionManager, Authenticator authenticator ) {
        super( transactionManager, authenticator );
        metricsSystemConfiguration = NoopMetricsSystemConfiguration.getInstance();
        metricsSystem = NoopMetricsSystem.getInstance();
    }


    @Override
    public void run() {
        try {
            final DbmsMeta meta = new DbmsMeta( transactionManager, authenticator );
            AvaticaHandler handler = new HandlerFactory().getHandler(
                    new DbmsService( meta, metricsSystem ),
                    Serialization.PROTOBUF,
                    metricsSystemConfiguration );
            final HttpServerDispatcher httpServerDispatcher = new HttpServerDispatcher( RuntimeConfig.JDBC_PORT.getInteger(), handler );
            httpServerDispatcher.start();
        } catch ( Exception e ) {
            log.error( "", e );
        }
    }
}
