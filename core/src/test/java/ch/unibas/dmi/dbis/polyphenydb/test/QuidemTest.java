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
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.test;


import static org.junit.Assert.fail;

import ch.unibas.dmi.dbis.polyphenydb.adapter.java.ReflectiveSchema;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionProperty;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.embedded.PolyphenyDbEmbeddedConnection;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Hook;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema.TableType;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.util.Bug;
import ch.unibas.dmi.dbis.polyphenydb.util.Closer;
import ch.unibas.dmi.dbis.polyphenydb.util.Sources;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.Lists;
import com.google.common.io.PatternFilenameFilter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem;
import org.apache.calcite.avatica.AvaticaUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;


/**
 * Test that runs every Quidem file as a test.
 */
@RunWith(Parameterized.class)
public abstract class QuidemTest {

    protected final String path;
    protected final Method method;


    /**
     * Creates a QuidemTest.
     */
    protected QuidemTest( String path ) {
        this.path = path;
        this.method = findMethod( path );
    }


    private static Object getEnv( String varName ) {
        switch ( varName ) {
            case "jdk18":
                return System.getProperty( "java.version" ).startsWith( "1.8" );
            case "fixed":
                // Quidem requires a Java 8 function
                return (Function<String, Object>) v -> {
                    switch ( v ) {
                        case "calcite1045":
                            return Bug.CALCITE_1045_FIXED;
                        case "calcite1048":
                            return Bug.CALCITE_1048_FIXED;
                    }
                    return null;
                };
            default:
                return null;
        }
    }


    private Method findMethod( String path ) {
        // E.g. path "sql/agg.iq" gives method "testSqlAgg"
        String methodName = AvaticaUtils.toCamelCase( "test_" + path.replace( File.separatorChar, '_' ).replaceAll( "\\.iq$", "" ) );
        Method m;
        try {
            m = getClass().getMethod( methodName );
        } catch ( NoSuchMethodException e ) {
            m = null;
        }
        return m;
    }


    protected static Collection<Object[]> data( String first ) {
        // inUrl = "file:/.../core/target/test-classes/sql/agg.iq"
        final URL inUrl = JdbcTest.class.getResource( "/" + n2u( first ) );
        final File firstFile = Sources.of( inUrl ).file();
        final int commonPrefixLength = firstFile.getAbsolutePath().length() - first.length();
        final File dir = firstFile.getParentFile();
        final List<String> paths = new ArrayList<>();
        final FilenameFilter filter = new PatternFilenameFilter( ".*\\.iq$" );
        for ( File f : Util.first( dir.listFiles( filter ), new File[0] ) ) {
            paths.add( f.getAbsolutePath().substring( commonPrefixLength ) );
        }
        return Lists.transform( paths, path -> new Object[]{ path } );
    }


    protected void checkRun( String path ) throws Exception {
        final File inFile;
        final File outFile;
        final File f = new File( path );
        if ( f.isAbsolute() ) {
            // e.g. path = "/tmp/foo.iq"
            inFile = f;
            outFile = new File( path + ".out" );
        } else {
            // e.g. path = "sql/outer.iq"
            // inUrl = "file:/.../core/target/test-classes/sql/outer.iq"
            final URL inUrl = JdbcTest.class.getResource( "/" + n2u( path ) );
            inFile = Sources.of( inUrl ).file();
            outFile = new File( inFile.getAbsoluteFile().getParent(), u2n( "surefire/" ) + path );
        }
        Util.discard( outFile.getParentFile().mkdirs() );
        try (
                Reader reader = Util.reader( inFile );
                Writer writer = Util.printWriter( outFile );
                Closer closer = new Closer()
        ) {
            final Quidem.Config config = Quidem.configBuilder()
                    .withReader( reader )
                    .withWriter( writer )
                    .withConnectionFactory( createConnectionFactory() )
                    .withCommandHandler( createCommandHandler() )
                    .withPropertyHandler( ( propertyName, value ) -> {
                        if ( propertyName.equals( "bindable" ) ) {
                            final boolean b = value instanceof Boolean && (Boolean) value;
                            closer.add( Hook.ENABLE_BINDABLE.addThread( Hook.propertyJ( b ) ) );
                        }
                        if ( propertyName.equals( "expand" ) ) {
                            final boolean b = value instanceof Boolean && (Boolean) value;
                            closer.add( Prepare.THREAD_EXPAND.push( b ) );
                        }
                    } )
                    .withEnv( QuidemTest::getEnv )
                    .build();
            new Quidem( config ).execute();
        }
        final String diff = DiffTestCase.diff( inFile, outFile );
        if ( !diff.isEmpty() ) {
            fail( "Files differ: " + outFile + " " + inFile + "\n" + diff );
        }
    }


    /**
     * Creates a command handler.
     */
    protected CommandHandler createCommandHandler() {
        return Quidem.EMPTY_COMMAND_HANDLER;
    }


    /**
     * Creates a connection factory.
     */
    protected Quidem.ConnectionFactory createConnectionFactory() {
        return new QuidemConnectionFactory();
    }


    /**
     * Converts a path from Unix to native. On Windows, converts forward-slashes to back-slashes; on Linux, does nothing.
     */
    private static String u2n( String s ) {
        return File.separatorChar == '\\'
                ? s.replace( '/', '\\' )
                : s;
    }


    private static String n2u( String s ) {
        return File.separatorChar == '\\'
                ? s.replace( '\\', '/' )
                : s;
    }


    @Test
    public void test() throws Exception {
        if ( method != null ) {
            try {
                method.invoke( this );
            } catch ( InvocationTargetException e ) {
                Throwable cause = e.getCause();
                if ( cause instanceof Exception ) {
                    throw (Exception) cause;
                }
                if ( cause instanceof Error ) {
                    throw (Error) cause;
                }
                throw e;
            }
        } else {
            checkRun( path );
        }
    }


    /**
     * Quidem connection factory for Polypheny-DB's built-in test schemas.
     */
    protected static class QuidemConnectionFactory implements Quidem.ConnectionFactory {

        public Connection connect( String name ) throws Exception {
            return connect( name, false );
        }


        public Connection connect( String name, boolean reference ) throws Exception {
            if ( reference ) {
                if ( name.equals( "foodmart" ) ) {
                    final ConnectionSpec db = PolyphenyDbAssert.DatabaseInstance.HSQLDB.foodmart;
                    final Connection connection = DriverManager.getConnection( db.url, db.username, db.password );
                    connection.setSchema( "foodmart" );
                    return connection;
                }
                return null;
            }
            switch ( name ) {
                case "hr":
                    return PolyphenyDbAssert.hr()
                            .connect();
                case "foodmart":
                    return PolyphenyDbAssert.that()
                            .with( PolyphenyDbAssert.Config.FOODMART_CLONE )
                            .connect();
                case "geo":
                    return PolyphenyDbAssert.that()
                            .with( PolyphenyDbAssert.Config.GEO )
                            .connect();
                case "scott":
                    return PolyphenyDbAssert.that()
                            .with( PolyphenyDbAssert.Config.SCOTT )
                            .connect();
                case "jdbc_scott":
                    return PolyphenyDbAssert.that()
                            .with( PolyphenyDbAssert.Config.JDBC_SCOTT )
                            .connect();
                case "post":
                    return PolyphenyDbAssert.that()
                            .with( PolyphenyDbAssert.Config.REGULAR )
                            .with( PolyphenyDbAssert.SchemaSpec.POST )
                            .connect();
                case "catchall":
                    return PolyphenyDbAssert.that()
                            .withSchema( "s", new ReflectiveSchema( new ReflectiveSchemaTest.CatchallSchema() ) )
                            .connect();
                case "orinoco":
                    return PolyphenyDbAssert.that()
                            .with( PolyphenyDbAssert.SchemaSpec.ORINOCO )
                            .connect();
                case "blank":
                    return PolyphenyDbAssert.that()
                            .with( PolyphenyDbConnectionProperty.PARSER_FACTORY, "ch.unibas.dmi.dbis.polyphenydb.sql.parser.parserextensiontesting.ExtensionSqlParserImpl#FACTORY" )
                            .with( PolyphenyDbAssert.SchemaSpec.BLANK )
                            .connect();
                case "seq":
                    final Connection connection = PolyphenyDbAssert.that()
                            .withSchema( "s", new AbstractSchema() )
                            .connect();
                    connection.unwrap( PolyphenyDbEmbeddedConnection.class ).getRootSchema()
                            .getSubSchema( "s" )
                            .add( "my_seq",
                                    new AbstractTable() {
                                        public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
                                            return typeFactory.builder().add( "$seq", SqlTypeName.BIGINT ).build();
                                        }


                                        @Override
                                        public TableType getJdbcTableType() {
                                            return Schema.TableType.SEQUENCE;
                                        }
                                    } );
                    return connection;
                case "server":
                    return DdlTest.connect();
                default:
                    throw new RuntimeException( "unknown connection '" + name + "'" );
            }
        }
    }

}
