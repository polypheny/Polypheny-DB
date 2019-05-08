/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
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
 */

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc.JdbcSchema;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionProperty;
import ch.unibas.dmi.dbis.polyphenydb.model.JsonSchema.Type;
import ch.unibas.dmi.dbis.polyphenydb.model.ModelHandler;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import ch.unibas.dmi.dbis.polyphenydb.util.JsonBuilder;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.BuiltInConnectionProperty;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Handler;
import org.apache.calcite.avatica.HandlerImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.linq4j.function.Function0;


/**
 * Polypheny-DB JDBC driver.
 */
public class EmbeddedDriver extends UnregisteredDriver {

    public static final String CONNECT_STRING_PREFIX = "jdbc:polyphenydbembedded:";

    final Function0<PolyphenyDbPrepare> prepareFactory;


    static {
        new EmbeddedDriver().register();
    }


    public EmbeddedDriver() {
        super();
        this.prepareFactory = createPrepareFactory();
    }


    protected Function0<PolyphenyDbPrepare> createPrepareFactory() {
        return PolyphenyDbPrepare.DEFAULT_FACTORY;
    }


    @Override
    protected String getConnectStringPrefix() {
        return CONNECT_STRING_PREFIX;
    }


    @Override
    protected String getFactoryClassName( JdbcVersion jdbcVersion ) {
        switch ( jdbcVersion ) {
            case JDBC_30:
            case JDBC_40:
                throw new IllegalArgumentException( "JDBC version not supported: " + jdbcVersion );
            case JDBC_41:
            default:
                return "ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbEmbeddedJdbc41Factory";
        }
    }


    protected DriverVersion createDriverVersion() {
        return DriverVersion.load(
                EmbeddedDriver.class,
                "ch-unibas-dmi-dbis-polyphenydb-jdbc.properties",
                "Polypheny-DB JDBC Embedded Driver",
                "unknown version",
                "Polypheny-DB",
                "unknown version" );
    }


    @Override
    protected Handler createHandler() {
        return new HandlerImpl() {
            @Override
            public void onConnectionInit( AvaticaConnection connection_ ) throws SQLException {
                final PolyphenyDbEmbeddedConnectionImpl connection = (PolyphenyDbEmbeddedConnectionImpl) connection_;
                super.onConnectionInit( connection );
                final String model = model( connection );
                if ( model != null ) {
                    try {
                        new ModelHandler( connection, model );
                    } catch ( IOException e ) {
                        throw new SQLException( e );
                    }
                }
                connection.init();
            }


            String model( PolyphenyDbEmbeddedConnectionImpl connection ) {
                String model = connection.config().model();
                if ( model != null ) {
                    return model;
                }
                SchemaFactory schemaFactory = connection.config().schemaFactory( SchemaFactory.class, null );
                final Properties info = connection.getProperties();
                final String schemaName = Util.first( connection.config().schema(), "adhoc" );
                if ( schemaFactory == null ) {
                    final Type schemaType = connection.config().schemaType();
                    if ( schemaType != null ) {
                        switch ( schemaType ) {
                            case JDBC:
                                schemaFactory = JdbcSchema.Factory.INSTANCE;
                                break;
                            case MAP:
                                schemaFactory = AbstractSchema.Factory.INSTANCE;
                                break;
                        }
                    }
                }
                if ( schemaFactory != null ) {
                    final JsonBuilder json = new JsonBuilder();
                    final Map<String, Object> root = json.map();
                    root.put( "version", "1.0" );
                    root.put( "defaultSchema", schemaName );
                    final List<Object> schemaList = json.list();
                    root.put( "schemas", schemaList );
                    final Map<String, Object> schema = json.map();
                    schemaList.add( schema );
                    schema.put( "type", "custom" );
                    schema.put( "name", schemaName );
                    schema.put( "factory", schemaFactory.getClass().getName() );
                    final Map<String, Object> operandMap = json.map();
                    schema.put( "operand", operandMap );
                    for ( Map.Entry<String, String> entry : Util.toMap( info ).entrySet() ) {
                        if ( entry.getKey().startsWith( "schema." ) ) {
                            operandMap.put( entry.getKey().substring( "schema.".length() ), entry.getValue() );
                        }
                    }
                    return "inline:" + json.toJsonString( root );
                }
                return null;
            }
        };
    }


    @Override
    protected Collection<ConnectionProperty> getConnectionProperties() {
        final List<ConnectionProperty> list = new ArrayList<>();
        Collections.addAll( list, BuiltInConnectionProperty.values() );
        Collections.addAll( list, PolyphenyDbConnectionProperty.values() );
        return list;
    }


    @Override
    public Meta createMeta( AvaticaConnection connection ) {
        return new PolyphenyDbEmbeddedMetaImpl( (PolyphenyDbEmbeddedConnectionImpl) connection );
    }


    /**
     * Creates an internal connection.
     */
    PolyphenyDbEmbeddedConnection connect( PolyphenyDbSchema rootSchema, JavaTypeFactory typeFactory ) {
        return (PolyphenyDbEmbeddedConnection) ((PolyphenyDbFactory) factory)
                .newConnection( this, factory, CONNECT_STRING_PREFIX, new Properties(), rootSchema, typeFactory );
    }


    /**
     * Creates an internal connection.
     */
    PolyphenyDbEmbeddedConnection connect( PolyphenyDbSchema rootSchema, JavaTypeFactory typeFactory, Properties properties ) {
        return (PolyphenyDbEmbeddedConnection) ((PolyphenyDbFactory) factory)
                .newConnection( this, factory, CONNECT_STRING_PREFIX, properties, rootSchema, typeFactory );
    }
}

