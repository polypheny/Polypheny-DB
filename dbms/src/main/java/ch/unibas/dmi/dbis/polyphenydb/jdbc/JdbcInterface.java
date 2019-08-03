/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.QueryInterface;
import ch.unibas.dmi.dbis.polyphenydb.TransactionManager;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.MetricsSystemConfiguration;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystemConfiguration;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.server.AvaticaHandler;
import org.apache.calcite.avatica.server.HandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JdbcInterface extends QueryInterface {

    private static final Logger LOG = LoggerFactory.getLogger( JdbcInterface.class );


    private final MetricsSystemConfiguration metricsSystemConfiguration;
    private final MetricsSystem metricsSystem;


    public JdbcInterface( TransactionManager transactionManager ) {
        super( transactionManager );
        metricsSystemConfiguration = NoopMetricsSystemConfiguration.getInstance();
        metricsSystem = NoopMetricsSystem.getInstance();
    }


    @Override
    public void run() {
        try {
            final DbmsMeta meta = new DbmsMeta( transactionManager );
            AvaticaHandler handler = new HandlerFactory().getHandler(
                    new DbmsService( meta, metricsSystem ),
                    Serialization.PROTOBUF,
                    metricsSystemConfiguration );
            final HttpServerDispatcher httpServerDispatcher = new HttpServerDispatcher( RuntimeConfig.JDBC_PORT.getInteger(), handler );
            httpServerDispatcher.start();
        } catch ( Exception e ) {
            LOG.error( "", e );
        }
    }
}
