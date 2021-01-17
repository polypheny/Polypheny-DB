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

package org.polypheny.db.config;


import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.polypheny.db.config.Config.ConfigListener;


public class ConfigManagerTest implements ConfigListener {

    private ConfigManager cm;
    private boolean wasRestarted = false;


    static {
        ConfigManager cm = ConfigManager.getInstance();
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


    @Test
    public void javaValidation() {
        Config c5 = new ConfigInteger( "java.int.validation", 10 ).withJavaValidation( a -> (int) a < 10 ).withUi( "g2" );
        Config c6 = new ConfigDouble( "java.double.validation", 3 ).withJavaValidation( a -> (double) a < 5.5 ).withUi( "g2" );

        cm.registerConfig( c5 );
        cm.registerConfig( c6 );

        cm.getConfig( "java.int.validation" ).setInt( 2 );
        cm.getConfig( "java.int.validation" ).setInt( 20 );
        int a = cm.getConfig( "java.int.validation" ).getInt();
        Assert.assertEquals( 2, a );
        cm.getConfig( "java.double.validation" ).setDouble( 1.2 );
        cm.getConfig( "java.double.validation" ).setDouble( 10.4 );
        Assert.assertEquals( 1.2, cm.getConfig( "java.double.validation" ).getDouble(), 0.01 );

        System.out.println( cm.getPage( "p" ) );
    }


    @Test
    public void configTypes() {
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

        Assert.assertEquals( "string", cm.getConfig( "type.string" ).getString() );
        Assert.assertTrue( cm.getConfig( "type.boolean" ).getBoolean() );
        Assert.assertEquals( 10, (int) cm.getConfig( "type.integer" ).getInt() );
        Assert.assertEquals( 11, (long) cm.getConfig( "type.long" ).getLong() );
        Assert.assertEquals( 10.1, cm.getConfig( "type.double" ).getDouble(), 0.0001 );
        Assert.assertEquals( new BigDecimal( "3.14" ), cm.getConfig( "type.decimal" ).getDecimal() );

    }


    @Test
    public void isNotified() {

        ConfigObserver o1 = new ConfigObserver();
        ConfigObserver o2 = new ConfigObserver();
        ConfigBoolean willChange = new ConfigBoolean( "will.change", true );
        cm.registerConfig( willChange );
        cm.getConfig( "will.change" ).addObserver( o1 );
        cm.getConfig( "will.change" ).addObserver( o2 );
        cm.getConfig( "will.change" ).removeObserver( o1 );
        cm.getConfig( "will.change" ).setBoolean( true );
        Assert.assertTrue( o2.wasNotified() );
        Assert.assertFalse( o1.wasNotified() );

    }


    @Test
    public void isRestarted() {
        Config c = new ConfigString( "test.restart", "restart" ).setRequiresRestart( true ).addObserver( this );
        cm.registerConfig( c );
        cm.getConfig( c.getKey() ).setString( "someValue" );
        Assert.assertTrue( this.wasRestarted );
    }


    private enum testEnum {FOO, BAR, FOO_BAR}


    @Test
    public void configEnum() {
        Config c = new ConfigEnum( "enum", "Test description", testEnum.class, testEnum.FOO );
        cm.registerConfig( c );
        ConfigObserver o = new ConfigObserver();
        cm.getConfig( "enum" ).addObserver( o );

        Assert.assertSame( testEnum.FOO, c.getEnum() );

        c.setEnum( testEnum.BAR );

        Assert.assertSame( testEnum.BAR, c.getEnum() );
        Assert.assertTrue( c.getEnumValues().contains( testEnum.FOO ) );
        Assert.assertTrue( c.getEnumValues().contains( testEnum.BAR ) );
        Assert.assertTrue( c.getEnumValues().contains( testEnum.FOO_BAR ) );

        c.setEnum( testEnum.FOO_BAR );
        Assert.assertSame( testEnum.FOO_BAR, c.getEnum() );

        Assert.assertTrue( o.wasNotified() );
        Assert.assertEquals( 2, o.n );
    }


    @Test
    public void configEnumList() {
        Config c = new ConfigEnumList( "enumList", "Test description", testEnum.class, ImmutableList.of( testEnum.BAR ) );
        cm.registerConfig( c );
        ConfigObserver o = new ConfigObserver();
        cm.getConfig( "enumList" ).addObserver( o );

        Assert.assertTrue( c.getEnumList().contains( testEnum.BAR ) );
        Assert.assertFalse( c.getEnumList().contains( testEnum.FOO ) );
        Assert.assertFalse( c.getEnumList().contains( testEnum.FOO_BAR ) );

        Assert.assertTrue( c.removeEnum( testEnum.BAR ) );
        Assert.assertFalse( c.removeEnum( testEnum.FOO ) );
        c.addEnum( testEnum.FOO );
        c.addEnum( testEnum.FOO_BAR );
        Assert.assertFalse( c.getEnumList().contains( testEnum.BAR ) );
        Assert.assertTrue( c.getEnumList().contains( testEnum.FOO ) );
        Assert.assertTrue( c.getEnumList().contains( testEnum.FOO_BAR ) );

        Assert.assertTrue( c.getEnumValues().contains( testEnum.FOO ) );
        Assert.assertTrue( c.getEnumValues().contains( testEnum.BAR ) );
        Assert.assertTrue( c.getEnumValues().contains( testEnum.FOO_BAR ) );

        Assert.assertTrue( o.wasNotified() );
        Assert.assertEquals( 4, o.n );
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
        Config c = new ConfigClazz( "clazz", TestClass.class, FooImplementation.class );
        cm.registerConfig( c );
        ConfigObserver o = new ConfigObserver();
        cm.getConfig( "clazz" ).addObserver( o );

        Assert.assertSame( FooImplementation.class, c.getClazz() );

        c.setClazz( BarImplementation.class );

        Assert.assertSame( BarImplementation.class, c.getClazz() );
        Assert.assertTrue( c.getClazzes().contains( FooImplementation.class ) );
        Assert.assertTrue( c.getClazzes().contains( BarImplementation.class ) );

        Assert.assertTrue( o.wasNotified() );
        Assert.assertEquals( 1, o.n );

        // The order in which gson serializes sets is not predictable. Disabling the serialization test to avoid randomly failing tests
        /*String json = c.toJson();
        Gson gson = new Gson();
        ConfigClazz z = gson.fromJson( json, ConfigClazz.class );
        Assert.assertEquals( json, z.toJson() );*/
    }


    @Test
    public void configClazzList() {
        List<Class> l = new ArrayList<>();
        l.add( FooImplementation.class );
        l.add( BarImplementation.class );
        Config c = new ConfigClazzList( "clazzList", TestClass.class, l );
        cm.registerConfig( c );
        ConfigObserver o = new ConfigObserver();
        cm.getConfig( "clazzList" ).addObserver( o );

        Assert.assertTrue( c.getClazzList().contains( FooImplementation.class ) );
        Assert.assertTrue( c.getClazzList().contains( BarImplementation.class ) );
        Assert.assertFalse( c.getClazzList().contains( FooBarImplementation.class ) );

        Assert.assertFalse( c.removeClazz( FooBarImplementation.class ) );
        c.addClazz( FooBarImplementation.class );
        Assert.assertTrue( c.removeClazz( BarImplementation.class ) );

        Assert.assertTrue( c.getClazzList().contains( FooImplementation.class ) );
        Assert.assertFalse( c.getClazzList().contains( BarImplementation.class ) );
        Assert.assertTrue( c.getClazzList().contains( FooBarImplementation.class ) );

        Assert.assertTrue( c.getClazzes().contains( FooImplementation.class ) );
        Assert.assertTrue( c.getClazzes().contains( BarImplementation.class ) );
        Assert.assertTrue( c.getClazzes().contains( FooBarImplementation.class ) );

        Assert.assertTrue( o.wasNotified() );
        Assert.assertEquals( 3, o.n );

        // The order in which gson serializes sets is not predictable. Disabling the serialization test to avoid randomly failing tests
        /*String json = c.toJson();
        Gson gson = new Gson();
        ConfigClazzList z = gson.fromJson( json, ConfigClazzList.class );
        Assert.assertEquals( json, z.toJson() );*/
    }


    @Test
    public void configArray() {
        int[] array = { 1, 2, 3, 4, 5 };
        Config c = new ConfigArray( "array", array );
        cm.registerConfig( c );
        ConfigObserver o = new ConfigObserver();
        cm.getConfig( "array" ).addObserver( o );

        int[] otherArray = { 5, 4, 3, 2, 1 };
        c.setIntArray( otherArray );

        Assert.assertArrayEquals( otherArray, c.getIntArray() );
        Assert.assertTrue( o.wasNotified() );
        Assert.assertEquals( 1, o.n );
    }


    @Test
    public void configTable() {
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

        Assert.assertArrayEquals( otherTable[0], c.getIntTable()[0] );
        Assert.assertTrue( o.wasNotified() );
        Assert.assertEquals( 1, o.n );
    }


    @Test
    public void configFiles() {
        // Check if it works for integer values
        Config c = new ConfigInteger( "test.junit.int", 18 );
        Assert.assertEquals( c.getInt(), 18 );
        cm.registerConfig( c );
        Assert.assertEquals( c.getInt(), 42 );

        // Check if it works for string values
        c = new ConfigString( "test.junit.string", "Hello World" );
        Assert.assertEquals( c.getString(), "Hello World" );
        cm.registerConfig( c );
        Assert.assertEquals( c.getString(), "Polypheny-DB" );

        // Check if it works for boolean values
        c = new ConfigBoolean( "test.junit.boolean", false );
        Assert.assertFalse( c.getBoolean() );
        cm.registerConfig( c );
        Assert.assertTrue( c.getBoolean() );

        // Check if it works for decimal values
        c = new ConfigDecimal( "test.junit.decimal", new BigDecimal( "2.22222222222" ) );
        Assert.assertEquals( c.getDecimal(), new BigDecimal( "2.22222222222" ) );
        cm.registerConfig( c );
        Assert.assertEquals( c.getDecimal(), new BigDecimal( "1.111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111234567891" ) );

        // Check if it works for double values
        c = new ConfigDouble( "test.junit.double", 1.11d );
        Assert.assertEquals( c.getDouble(), 1.11d, 0.0 );
        cm.registerConfig( c );
        Assert.assertEquals( c.getDouble(), -3.0d, 0.0 );

        // Check if it works for long values
        c = new ConfigLong( "test.junit.long", Integer.MAX_VALUE + 152463L );
        Assert.assertEquals( c.getLong(), Integer.MAX_VALUE + 152463L );
        cm.registerConfig( c );
        Assert.assertEquals( c.getLong(), Integer.MAX_VALUE + 9999999L );

        // Check if it works for Enum values
        c = new ConfigEnum( "test.junit.enum", "Test description", testEnum.class, testEnum.FOO_BAR );
        Assert.assertEquals( c.getEnum(), testEnum.FOO_BAR );
        cm.registerConfig( c );
        Assert.assertEquals( c.getEnum(), testEnum.FOO );

        // Check if it works for Enum Lists
        c = new ConfigEnumList( "test.junit.enumList", "Test description", testEnum.class, ImmutableList.of( testEnum.FOO_BAR ) );
        Assert.assertEquals( c.getEnumList(), ImmutableList.of( testEnum.FOO_BAR ) );
        cm.registerConfig( c );
        Assert.assertEquals( c.getEnumList(), ImmutableList.of( testEnum.FOO, testEnum.BAR ) );

        // Check if it works for Class values
        c = new ConfigClazz( "test.junit.class", TestClass.class, FooBarImplementation.class );
        Assert.assertEquals( c.getClazz(), FooBarImplementation.class );
        cm.registerConfig( c );
        Assert.assertEquals( c.getClazz(), FooImplementation.class );

        // Check if it works for Class Lists
        c = new ConfigClazzList( "test.junit.classList", TestClass.class, ImmutableList.of( FooBarImplementation.class ) );
        Assert.assertEquals( c.getClazzList(), ImmutableList.of( FooBarImplementation.class ) );
        cm.registerConfig( c );
        Assert.assertEquals( c.getClazzList(), ImmutableList.of( FooImplementation.class, BarImplementation.class ) );

        // ----

        // Check for a key that is not present in the config file
        c = new ConfigString( "mot.in.config.file", "Polystore" );
        Assert.assertEquals( c.getString(), "Polystore" );
        cm.registerConfig( c );
        Assert.assertEquals( c.getString(), "Polystore" );

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
