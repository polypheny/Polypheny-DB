package ch.unibas.dmi.dbis.polyphenydb.webui;

import ch.unibas.dmi.dbis.polyphenydb.config.*;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigManager.Restartable;
import java.math.BigDecimal;
import org.junit.Assert;
import org.junit.Test;

public class ConfigManagerTest implements Restartable {

    private ConfigManager cm;
    private boolean wasRestarted = false;

    public ConfigManagerTest () {
        //insert config before groups and pages are existing
        cm = ConfigManager.getInstance().observeRestart( this );
        WebUiPage p = new WebUiPage( 1, "page1", "page1descr" );
        WebUiGroup g1 = new WebUiGroup( 1, 1 ).withTitle( "group1" );
        WebUiGroup g2 = new WebUiGroup( 2, 1 ).withDescription( "group2" );

        ConfigString c1 = new ConfigString("conf.text.1");
        cm.registerConfig( c1 );
        cm.addUiGroup( g2 );
        cm.addUiGroup( g1 );
        cm.addUiPage( p );
    }

    @Test
    public void insertConfigTwice() {

        Config c1 = new ConfigString("conf.text.1").withUi( 1, WebUiFormType.TEXT );
        Config c2 = new ConfigString("conf.text.1");

        Config c3 = new ConfigInteger( "double" );
        Config c4 = new ConfigInteger( "double" ).withUi( 1, WebUiFormType.NUMBER );

        //inserting configs before groups and pages are existing
        cm.registerConfig( c1 );
        cm.registerConfig( c2 );
        cm.registerConfig( c3 );
        cm.registerConfig( c4 );

        cm.setConfigValue( "conf.text.1", "config1" );
        //c1.setString( "config1" );//wouldn't work
        cm.setConfigValue( "double", 22 );
        Assert.assertEquals( "config1", cm.getConfig( "conf.text.1" ).getString() );
        Assert.assertEquals( 1, cm.getConfig( "double" ).getWebUiGroup() );

    }

    @Test
    public void javaValidation () {
        Config c5 = new ConfigInteger( "java.int.validation" ).withJavaValidation( a -> a < 10 ).withUi( 2, WebUiFormType.NUMBER );
        Config c6 = new ConfigInteger( "java.number.validation" ).withJavaValidation( a -> a < 10 ).withUi( 2, WebUiFormType.NUMBER );

        cm.registerConfig( c5 );
        cm.registerConfig( c6 );

        cm.setConfigValue( "java.int.validation", 2 );
        cm.setConfigValue( "java.int.validation", 20 );
        int a = cm.getConfig( "java.int.validation" ).getInt();
        Assert.assertEquals( 2, a );
        cm.setConfigValue( "java.number.validation", 3 );
        cm.setConfigValue( "java.number.validation", 20 );
        //Assert.assertEquals( 2, cm.getConfig( "java.number.validation" ).getInt() );

        System.out.println( cm.getPage( 1 ) );
    }

    @Test
    public void configTypes () {
        Config c1 = new ConfigNull("type.null");
        Config c2 = new ConfigString("type.string");
        Config c3 = new ConfigBoolean("type.boolean");
        Config c4 = new ConfigInteger("type.integer");
        Config c5 = new ConfigLong("type.long");
        Config c6 = new ConfigDouble( "type.double" );
        Config c7 = new ConfigDecimal("type.decimal");

        c1.setObject( null );
        c2.setString( "string" );
        c3.setBoolean( true );
        c4.setInt( 10 );
        c5.setLong( 11 );
        c6.setDouble( 10.1 );
        c7.setDecimal( new BigDecimal( 3.14 ) );

        cm.registerConfigs( c1, c2, c3, c4, c5, c6, c7 );

        Assert.assertNull( null, cm.getConfig( "type.null" ).getObject() );
        Assert.assertEquals( "string", cm.getConfig( "type.string" ).getString() );
        Assert.assertTrue( cm.getConfig( "type.boolean" ).getBoolean() );
        Assert.assertEquals( 10, (int) cm.getConfig(  "type.integer" ).getInt() );
        Assert.assertEquals( 11, (long) cm.getConfig(  "type.long" ).getLong() );
        Assert.assertEquals( 10.1, cm.getConfig(  "type.double" ).getDouble(), 0.0001 );
        Assert.assertEquals( new BigDecimal( 3.14 ), cm.getConfig(  "type.decimal" ).getDecimal() );

    }

    @Test
    public void isRestarted () {
        Config c = new ConfigString( "test.restart" ).setRequiresRestart();
        cm.registerConfig( c );
        cm.setConfigValue( c.getKey(), "someValue" );
        Assert.assertEquals( true, this.wasRestarted );
    }

    public void restart(){
        this.wasRestarted = true;
    }

}
