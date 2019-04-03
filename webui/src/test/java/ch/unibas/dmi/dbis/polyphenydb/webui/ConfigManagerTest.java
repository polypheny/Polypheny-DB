package ch.unibas.dmi.dbis.polyphenydb.webui;

import ch.unibas.dmi.dbis.polyphenydb.config.*;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigInteger.ConfigValidator;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigManager.Restartable;
import org.junit.Assert;
import org.junit.Test;

public class ConfigManagerTest implements Restartable {

    private ConfigManager cm;
    private WebUiPage p;
    private WebUiGroup g1;
    private WebUiGroup g2;
    private boolean wasRestarted = false;

    public ConfigManagerTest () {
        //insert config before groups and pages are existing
        cm = ConfigManager.getInstance().observeRestart( this );
        p = new WebUiPage( 1, "page1", "page1descr" );
        g1 = new WebUiGroup( 1, 1 ).withTitle( "group1" );
        g2 = new WebUiGroup( 2, 1 ).withDescription( "group2" );

        ConfigString c1 = new ConfigString("conf.text.1");
        cm.registerConfig( c1 );
        cm.addUiGroup( g2 );
        cm.addUiGroup( g1 );
        cm.addUiPage( p );
    }

    @Test
    public void insertConfigTwice() {

        ConfigString c1 = new ConfigString("conf.text.1").withUi( 1, WebUiFormType.TEXT );
        ConfigString c2 = new ConfigString("conf.text.1");

        ConfigInteger c3 = new ConfigInteger( "double" );
        ConfigInteger c4 = new ConfigInteger( "double" ).withUi( 1, WebUiFormType.NUMBER );

        //inserting configs before groups and pages are existing
        cm.registerConfig( c1 );
        cm.registerConfig( c2 );
        cm.registerConfig( c3 );
        cm.registerConfig( c4 );

        cm.setConfigValue( "conf.text.1", "config1" );
        //c1.setValue( "config1" );//wouldn't work
        Assert.assertEquals( "config1", cm.getConfig( "conf.text.1" ).getValue() );
        Assert.assertEquals( 1, cm.getConfig( "double" ).getWebUiGroup() );

        System.out.println( cm.getPage(1) );

    }

    @Test
    public void javaValidation () {
        ConfigInteger c5 = new ConfigInteger( "java.int.validation" ).withJavaValidation( a -> a < 10 );
        ConfigNumber c6 = new ConfigNumber( "java.number.validation" ).withJavaValidation( a -> a.doubleValue() < 10 );

        cm.registerConfig( c5 );
        cm.registerConfig( c6 );

        cm.setConfigValue( "java.int.validation", 2 );
        cm.setConfigValue( "java.int.validation", 20 );
        Assert.assertEquals( 2, cm.getConfig( "java.int.validation" ).getValue() );
        cm.setConfigValue( "java.number.validation", 2 );
        cm.setConfigValue( "java.number.validation", 20 );
        Assert.assertEquals( 2, cm.getConfig( "java.number.validation" ).getValue() );
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
