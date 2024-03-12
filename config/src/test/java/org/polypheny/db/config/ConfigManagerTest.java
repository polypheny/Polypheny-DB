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

package org.polypheny.db.config;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.exception.ConfigRuntimeException;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;


public class ConfigManagerTest implements ConfigListener {

    private final ConfigManager cm;
    private boolean wasRestarted = false;


    @BeforeAll
    public static void setupTestEnvironment() {
        PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        ConfigManager cm = ConfigManager.getInstance();

        // Check if the correct file will be accessed
        Path originFile = Paths.get( "src/test/resources/application.conf" );
        Path testFile = Paths.get( "src/test/resources/base_test.conf" );

        try {
            Files.copy( originFile.toFile(), testFile.toFile() );
        } catch ( IOException e ) {
            e.printStackTrace();
        }
        ConfigManager.setApplicationConfFile( testFile.toFile() );

        WebUiPage p = new WebUiPage( "p", "page1", "page1descr" );
        WebUiGroup g1 = new WebUiGroup( "g1", "p", 2 ).withTitle( "group1" );
        WebUiGroup g2 = new WebUiGroup( "g2", "p", 1 ).withDescription( "group2" );

        Config c1 = new ConfigString( "conf.test.2", "text1" ).withUi( "g2" );
        cm.registerConfig( c1 );
        cm.registerWebUiGroup( g2 );
        cm.registerWebUiGroup( g1 );
        cm.registerWebUiPage( p );
    }


    public ConfigManagerTest() {
        //insert config before groups and pages are existing
        cm = ConfigManager.getInstance();
    }


    @AfterAll
    public static void resetTestEnvironment() {
        ConfigManager.getInstance().useDefaultApplicationConfFile();
    }


    @Test
    public void javaValidation() {
        // Check if the correct file will be accessed
        Path originFile = Paths.get( "src/test/resources/application.conf" );
        Path testFile = Paths.get( "src/test/resources/java_validation_test.conf" );

        try {
            try {
                Files.copy( originFile.toFile(), testFile.toFile() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            cm.setApplicationConfFile( testFile.toFile() );

            Config c5 = new ConfigInteger( "java.int.validation", 10 ).withJavaValidation( a -> (int) a < 10 ).withUi( "g2" );
            Config c6 = new ConfigDouble( "java.double.validation", 3 ).withJavaValidation( a -> (double) a < 5.5 ).withUi( "g2" );

            cm.registerConfig( c5 );
            cm.registerConfig( c6 );

            cm.getConfig( "java.int.validation" ).setInt( 2 );
            cm.getConfig( "java.int.validation" ).setInt( 20 );
            int a = cm.getConfig( "java.int.validation" ).getInt();
            assertEquals( 2, a );
            cm.getConfig( "java.double.validation" ).setDouble( 1.2 );
            cm.getConfig( "java.double.validation" ).setDouble( 10.4 );
            assertEquals( 1.2, cm.getConfig( "java.double.validation" ).getDouble(), 0.01 );

            System.out.println( cm.getPage( "p" ) );

        } finally {
            testFile.toFile().delete();
        }
    }


    @Test
    public void configTypes() {

        // Check if the correct file will be accessed
        Path originFile = Paths.get( "src/test/resources/application.conf" );
        Path testFile = Paths.get( "src/test/resources/config_types_test.conf" );

        try {
            try {
                Files.copy( originFile.toFile(), testFile.toFile() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            cm.setApplicationConfFile( testFile.toFile() );

            Config c2 = new ConfigString( "type.string", "string" );
            Config c3 = new ConfigBoolean( "type.boolean", true );
            Config c4 = new ConfigInteger( "type.integer", 11 );
            Config c5 = new ConfigLong( "type.long", 100 );
            Config c6 = new ConfigDouble( "type.double", 1.01 );
            Config c7 = new ConfigDecimal( "type.decimal", new BigDecimal( "1.0001" ) );

            c2.setString( "string" );
            c3.setBoolean( true );
            c4.setInt( 10 );
            c5.setLong( 11 );
            c6.setDouble( 10.1 );
            c7.setDecimal( new BigDecimal( "3.14" ) );

            cm.registerConfigs( c2, c3, c4, c5, c6, c7 );

            assertEquals( "string", cm.getConfig( "type.string" ).getString() );
            assertTrue( cm.getConfig( "type.boolean" ).getBoolean() );
            assertEquals( 10, (int) cm.getConfig( "type.integer" ).getInt() );
            assertEquals( 11, (long) cm.getConfig( "type.long" ).getLong() );
            assertEquals( 10.1, cm.getConfig( "type.double" ).getDouble(), 0.0001 );
            assertEquals( new BigDecimal( "3.14" ), cm.getConfig( "type.decimal" ).getDecimal() );
        } finally {
            testFile.toFile().delete();
        }
    }


    @Test
    public void isNotified() {
// Check if the correct file will be accessed
        Path originFile = Paths.get( "src/test/resources/application.conf" );
        Path testFile = Paths.get( "src/test/resources/notification_test.conf" );

        try {
            try {
                Files.copy( originFile.toFile(), testFile.toFile() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            cm.setApplicationConfFile( testFile.toFile() );

            ConfigObserver o1 = new ConfigObserver();
            ConfigObserver o2 = new ConfigObserver();
            ConfigBoolean willChange = new ConfigBoolean( "will.change", true );
            cm.registerConfig( willChange );
            cm.getConfig( "will.change" ).addObserver( o1 );
            cm.getConfig( "will.change" ).addObserver( o2 );
            cm.getConfig( "will.change" ).removeObserver( o1 );
            cm.getConfig( "will.change" ).setBoolean( true );
            assertTrue( o2.wasNotified() );
            assertFalse( o1.wasNotified() );
        } finally {
            testFile.toFile().delete();
        }
    }


    @Test
    public void isRestarted() {
        // Check if the correct file will be accessed
        Path originFile = Paths.get( "src/test/resources/application.conf" );
        Path testFile = Paths.get( "src/test/resources/restarted_test.conf" );

        try {
            try {
                Files.copy( originFile.toFile(), testFile.toFile() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            cm.setApplicationConfFile( testFile.toFile() );

            Config c = new ConfigString( "test.restart", "restart" ).requiresRestart( true ).addObserver( this );
            cm.registerConfig( c );
            cm.getConfig( c.getKey() ).setString( "someValue" );
            assertTrue( this.wasRestarted );
        } finally {
            testFile.toFile().delete();
        }
    }


    private enum testEnum {FOO, BAR, FOO_BAR}


    @Test
    public void configEnum() {
        // Check if the correct file will be accessed
        Path originFile = Paths.get( "src/test/resources/application.conf" );
        Path testFile = Paths.get( "src/test/resources/enum_test.conf" );

        try {
            try {
                Files.copy( originFile.toFile(), testFile.toFile() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            cm.setApplicationConfFile( testFile.toFile() );

            Config c = new ConfigEnum( "enum", "Test description", testEnum.class, testEnum.FOO );
            cm.registerConfig( c );
            ConfigObserver o = new ConfigObserver();
            cm.getConfig( "enum" ).addObserver( o );

            assertSame( testEnum.FOO, c.getEnum() );

            c.setEnum( testEnum.BAR );

            assertSame( testEnum.BAR, c.getEnum() );
            assertTrue( c.getEnumValues().contains( testEnum.FOO ) );
            assertTrue( c.getEnumValues().contains( testEnum.BAR ) );
            assertTrue( c.getEnumValues().contains( testEnum.FOO_BAR ) );

            c.setEnum( testEnum.FOO_BAR );
            assertSame( testEnum.FOO_BAR, c.getEnum() );

            assertTrue( o.wasNotified() );
            assertEquals( 2, o.n );

        } finally {
            testFile.toFile().delete();
        }
    }


    @Test
    public void configEnumList() {

        // Check if the correct file will be accessed
        Path originFile = Paths.get( "src/test/resources/application.conf" );
        Path testFile = Paths.get( "src/test/resources/enum_list_test.conf" );

        try {
            try {
                Files.copy( originFile.toFile(), testFile.toFile() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            cm.setApplicationConfFile( testFile.toFile() );

            Config c = new ConfigEnumList( "enumList", "Test description", testEnum.class, ImmutableList.of( testEnum.BAR ) );
            cm.registerConfig( c );
            ConfigObserver o = new ConfigObserver();
            cm.getConfig( "enumList" ).addObserver( o );

            assertTrue( c.getEnumList().contains( testEnum.BAR ) );
            assertFalse( c.getEnumList().contains( testEnum.FOO ) );
            assertFalse( c.getEnumList().contains( testEnum.FOO_BAR ) );

            assertTrue( c.removeEnum( testEnum.BAR ) );
            assertFalse( c.removeEnum( testEnum.FOO ) );
            c.addEnum( testEnum.FOO );
            c.addEnum( testEnum.FOO_BAR );
            assertFalse( c.getEnumList().contains( testEnum.BAR ) );
            assertTrue( c.getEnumList().contains( testEnum.FOO ) );
            assertTrue( c.getEnumList().contains( testEnum.FOO_BAR ) );

            assertTrue( c.getEnumValues().contains( testEnum.FOO ) );
            assertTrue( c.getEnumValues().contains( testEnum.BAR ) );
            assertTrue( c.getEnumValues().contains( testEnum.FOO_BAR ) );

            assertTrue( o.wasNotified() );
            assertEquals( 4, o.n );

        } finally {
            testFile.toFile().delete();
        }
    }


    private static class TestClass {

        int a;

    }


    private static class FooImplementation extends TestClass {

        int b;

    }


    private static class BarImplementation extends TestClass {

        int c;

    }


    private static class FooBarImplementation extends TestClass {

        int d;

    }


    @Test
    public void configClazz() {

        // Check if the correct file will be accessed
        Path originFile = Paths.get( "src/test/resources/application.conf" );
        Path testFile = Paths.get( "src/test/resources/clazz_test.conf" );

        try {
            try {
                Files.copy( originFile.toFile(), testFile.toFile() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            cm.setApplicationConfFile( testFile.toFile() );

            Config c = new ConfigClazz( "clazz", TestClass.class, FooImplementation.class );
            cm.registerConfig( c );
            ConfigObserver o = new ConfigObserver();
            cm.getConfig( "clazz" ).addObserver( o );

            assertSame( FooImplementation.class, c.getClazz() );

            c.setClazz( BarImplementation.class );

            assertSame( BarImplementation.class, c.getClazz() );
            assertTrue( c.getClazzes().contains( FooImplementation.class ) );
            assertTrue( c.getClazzes().contains( BarImplementation.class ) );

            assertTrue( o.wasNotified() );
            assertEquals( 1, o.n );

            // The order in which gson serializes sets is not predictable. Disabling the serialization test to avoid randomly failing tests
            /*String json = c.toJson();
            Gson gson = new Gson();
            ConfigClazz z = gson.fromJson( json, ConfigClazz.class );
            assertEquals( json, z.toJson() );*/
        } finally {
            testFile.toFile().delete();
        }
    }


    @Test
    public void configClazzList() {

        // Check if the correct file will be accessed
        Path originFile = Paths.get( "src/test/resources/application.conf" );
        Path testFile = Paths.get( "src/test/resources/clazz_list_test.conf" );

        try {
            try {
                Files.copy( originFile.toFile(), testFile.toFile() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            cm.setApplicationConfFile( testFile.toFile() );

            List<Class> l = new ArrayList<>();
            l.add( FooImplementation.class );
            l.add( BarImplementation.class );
            Config c = new ConfigClazzList( "clazzList", TestClass.class, l );
            cm.registerConfig( c );
            ConfigObserver o = new ConfigObserver();
            cm.getConfig( "clazzList" ).addObserver( o );

            assertTrue( c.getClazzList().contains( FooImplementation.class ) );
            assertTrue( c.getClazzList().contains( BarImplementation.class ) );
            assertFalse( c.getClazzList().contains( FooBarImplementation.class ) );

            assertFalse( c.removeClazz( FooBarImplementation.class ) );
            c.addClazz( FooBarImplementation.class );
            assertTrue( c.removeClazz( BarImplementation.class ) );

            assertTrue( c.getClazzList().contains( FooImplementation.class ) );
            assertFalse( c.getClazzList().contains( BarImplementation.class ) );
            assertTrue( c.getClazzList().contains( FooBarImplementation.class ) );

            assertTrue( c.getClazzes().contains( FooImplementation.class ) );
            assertTrue( c.getClazzes().contains( BarImplementation.class ) );
            assertTrue( c.getClazzes().contains( FooBarImplementation.class ) );

            assertTrue( o.wasNotified() );
            assertEquals( 3, o.n );

            // The order in which gson serializes sets is not predictable. Disabling the serialization test to avoid randomly failing tests
            /*String json = c.toJson();
            Gson gson = new Gson();
            ConfigClazzList z = gson.fromJson( json, ConfigClazzList.class );
            assertEquals( json, z.toJson() );*/

        } finally {
            testFile.toFile().delete();
        }
    }


    @Test
    public void configArray() {

        // Check if the correct file will be accessed
        Path originFile = Paths.get( "src/test/resources/application.conf" );
        Path testFile = Paths.get( "src/test/resources/array_test.conf" );

        try {
            try {
                Files.copy( originFile.toFile(), testFile.toFile() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            cm.setApplicationConfFile( testFile.toFile() );

            int[] array = { 1, 2, 3, 4, 5 };
            Config c = new ConfigArray( "array", array );
            cm.registerConfig( c );
            ConfigObserver o = new ConfigObserver();
            cm.getConfig( "array" ).addObserver( o );

            int[] otherArray = { 5, 4, 3, 2, 1 };
            c.setIntArray( otherArray );

            assertArrayEquals( otherArray, c.getIntArray() );
            assertTrue( o.wasNotified() );
            assertEquals( 1, o.n );

        } finally {
            testFile.toFile().delete();
        }
    }


    @Test
    public void configTable() {
        // Check if the correct file will be accessed
        Path originFile = Paths.get( "src/test/resources/application.conf" );
        Path testFile = Paths.get( "src/test/resources/table_test.conf" );

        try {
            try {
                Files.copy( originFile.toFile(), testFile.toFile() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            cm.setApplicationConfFile( testFile.toFile() );

            int[][] table = new int[][]{
                    { 1, 2, 3 },
                    { 4, 5, 6 }
            };
            Config c = new ConfigTable( "table", table );
            cm.registerConfig( c );
            ConfigObserver o = new ConfigObserver();
            cm.getConfig( "table" ).addObserver( o );

            int[][] otherTable = new int[][]{
                    { 6, 5, 4 },
                    { 3, 2, 1 }
            };
            c.setIntTable( otherTable );

            assertArrayEquals( otherTable[0], c.getIntTable()[0] );
            assertTrue( o.wasNotified() );
            assertEquals( 1, o.n );

        } finally {
            testFile.toFile().delete();
        }
    }


    /**
     * Read from test configuration file and check if the values specified in configuration are used instead of default one
     */
    @Test
    public void configFiles() {

        // Check if the correct file will be accessed
        Path originFile = Paths.get( "src/test/resources/application.conf" );
        Path testFile = Paths.get( "src/test/resources/base_test.conf" );

        try {
            try {
                Files.copy( originFile.toFile(), testFile.toFile() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            cm.setApplicationConfFile( testFile.toFile() );
            assertEquals( testFile.toFile().getAbsolutePath(), cm.getActiveConfFile() );

            // Check if it works for integer values
            Config c = new ConfigInteger( "test.junit.int", 18 );
            assertEquals( c.getInt(), 18 );
            cm.registerConfig( c );
            assertEquals( c.getInt(), 42 );

            // Check if it works for string values
            c = new ConfigString( "test.junit.string", "Hello World" );
            assertEquals( c.getString(), "Hello World" );
            cm.registerConfig( c );
            assertEquals( c.getString(), "Polypheny-DB" );

            // Check if it works for boolean values
            c = new ConfigBoolean( "test.junit.boolean", false );
            assertFalse( c.getBoolean() );
            cm.registerConfig( c );
            assertTrue( c.getBoolean() );

            // Check if it works for decimal values
            c = new ConfigDecimal( "test.junit.decimal", new BigDecimal( "2.22222222222" ) );
            assertEquals( c.getDecimal(), new BigDecimal( "2.22222222222" ) );
            cm.registerConfig( c );
            assertEquals( c.getDecimal(), new BigDecimal( "1.111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111234567891" ) );

            // Check if it works for double values
            c = new ConfigDouble( "test.junit.double", 1.11d );
            assertEquals( c.getDouble(), 1.11d, 0.0 );
            cm.registerConfig( c );
            assertEquals( c.getDouble(), -3.0d, 0.0 );

            // Check if it works for long values
            c = new ConfigLong( "test.junit.long", Integer.MAX_VALUE + 152463L );
            assertEquals( c.getLong(), Integer.MAX_VALUE + 152463L );
            cm.registerConfig( c );
            assertEquals( c.getLong(), Integer.MAX_VALUE + 9999999L );

            // Check if it works for Enum values
            c = new ConfigEnum( "test.junit.enum", "Test description", testEnum.class, testEnum.FOO_BAR );
            assertEquals( c.getEnum(), testEnum.FOO_BAR );
            cm.registerConfig( c );
            assertEquals( c.getEnum(), testEnum.FOO );

            // Check if it works for Enum Lists
            c = new ConfigEnumList( "test.junit.enumList", "Test description", testEnum.class, ImmutableList.of( testEnum.FOO_BAR ) );
            assertEquals( c.getEnumList(), ImmutableList.of( testEnum.FOO_BAR ) );
            cm.registerConfig( c );
            assertEquals( c.getEnumList(), ImmutableList.of( testEnum.FOO, testEnum.BAR ) );

            // Check if it works for Class values
            c = new ConfigClazz( "test.junit.class", TestClass.class, FooBarImplementation.class );
            assertEquals( c.getClazz(), FooBarImplementation.class );
            cm.registerConfig( c );
            assertEquals( c.getClazz(), FooImplementation.class );

            // Check if it works for Class Lists
            c = new ConfigClazzList( "test.junit.classList", TestClass.class, ImmutableList.of( FooBarImplementation.class ) );
            assertEquals( c.getClazzList(), ImmutableList.of( FooBarImplementation.class ) );
            cm.registerConfig( c );
            assertEquals( c.getClazzList(), ImmutableList.of( FooImplementation.class, BarImplementation.class ) );

            // ----

            // Check for a key that is not present in the config file
            c = new ConfigString( "mot.in.config.file", "Polystore" );
            assertEquals( c.getString(), "Polystore" );
            cm.registerConfig( c );
            assertEquals( c.getString(), "Polystore" );
        } finally {
            testFile.toFile().delete();
        }
    }


    /**
     * Read and write to test configuration file and check if the values specified in configuration are used and can be dynamically changed
     */

    @Test
    public void configFilePersistence() throws IOException {

        // Specify an unknown file location
        boolean failed = false;
        try {
            cm.setApplicationConfFile( new File( "src/false/location/application.conf" ) );
        } catch ( ConfigRuntimeException e ) {
            failed = true;
        }
        assertTrue( failed );
        failed = false;

        // Check if the correct file will be accessed
        Path originFile = Paths.get( "src/test/resources/application.conf" );
        Path testFile = Paths.get( "src/test/resources/base_application.conf" );

        Path backupFile = Paths.get( "src/test/resources/base_application.conf.bkp" );
        try {
            try {
                Files.copy( originFile.toFile(), testFile.toFile() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }

            File customConfFile = new File( testFile.toString() );
            cm.setApplicationConfFile( customConfFile );
            assertEquals( customConfFile.getAbsolutePath(), cm.getActiveConfFile() );

            // Check if file will be re-created after config change

            // Check if CUSTOM file will be re-created after config change

            // Rename file to simulate "delete"
            Files.move( testFile.toFile(), backupFile.toFile() );
            Config c = new ConfigInteger( "test.my.dummy", 5000 );

            assertEquals( c.getInt(), 5000 );

            // Check if file is still present after registration
            cm.registerConfig( c );
            assertEquals( cm.getConfig( "test.my.dummy" ).getInt(), 5000 );

            // Modify config value in CUSTOM value
            c.setInt( 5001 );
            assertEquals( c.getInt(), 5001 );

            // Check if file was recreated
            // with new updated value as well as old values that were in the file before

        } finally {

            // Remove file that has been created in the meantime
            testFile.toFile().delete();
            backupFile.toFile().delete();
            //Files.move( backupFile.toFile(), testFile.toFile() );
        }

        // Check if config changes are correctly written to file

        // After several config changes back and forth, verify that indeed only the config
        // Valid config changes exist in config file

        // Verify that default values are not written to config

        // Change config to non-default
        // Verify that it is present in file

        // Change entry back to default and verify that it is not present anymore

    }


    @Test
    public void configDocker() {
        // Check if the correct file will be accessed
        Path originFile = Paths.get( "src/test/resources/application.conf" );
        Path testFile = Paths.get( "src/test/resources/docker_test.conf" );

        try {
            try {
                Files.copy( originFile.toFile(), testFile.toFile() );
            } catch ( IOException e ) {
                e.printStackTrace();
            }
            cm.setApplicationConfFile( testFile.toFile() );

            String url = "test";
            String alias = "name";
            ConfigDocker c = new ConfigDocker( 0, url, alias, "docker.io", 7001, 7002, 7003 );
            assertEquals( c.getAlias(), alias );
            assertEquals( c.getHost(), url );
            assertEquals( c.getRegistry(), "docker.io" );
            assertEquals( 7001, c.getCommunicationPort() );
            assertEquals( 7002, c.getHandshakePort() );
            assertEquals( 7003, c.getProxyPort() );

            cm.registerConfig( c );
            assertEquals( cm.getConfig( c.getKey() ), c );

        } finally {
            testFile.toFile().delete();
        }
    }


    @Override
    public void onConfigChange( Config c ) {
        System.out.println( "configChange: " + c.getKey() );
    }


    @Override
    public void restart( Config c ) {
        System.out.println( "Config " + c.getKey() + " triggered restart;" );
        this.wasRestarted = true;
    }


    static class ConfigObserver implements ConfigListener {

        private boolean wasNotified = false;

        /**
         * how many times it was notified
         */
        int n = 0;


        @Override
        public void restart( Config c ) {
            this.wasNotified = true;
        }


        @Override
        public void onConfigChange( Config c ) {
            this.wasNotified = true;
            this.n++;
        }


        public boolean wasNotified() {
            return this.wasNotified;
        }

    }

}
