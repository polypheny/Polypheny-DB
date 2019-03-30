package ch.unibas.dmi.dbis.polyphenydb.webui;

import org.junit.Assert;
import org.junit.Test;

public class ConfigManagerTest {

    @Test
    public void insertConfigTwice() {

        WebUiPage p = new WebUiPage( 1, "page1", "page1descr" );
        WebUiGroup g1 = new WebUiGroup( 1, 1 ).withTitle( "group1" );
        WebUiGroup g2 = new WebUiGroup( 2, 1 ).withDescription( "group2" );
        Config c1 = new Config("conf.text.1").withUi( 1, WebUiFormType.TEXT );
        Config c2 = new Config("conf.text.2").withUi( 1, WebUiFormType.TEXT );

        Config c3 = new Config( "double" );
        Config c4 = new Config( "double" ).withUi( 1, WebUiFormType.NUMBER );

        ConfigManager cm = ConfigManager.getInstance();

        //inserting configs before groups and pages are existing
        cm.registerConfig( c1 );
        cm.registerConfig( c2 );
        cm.registerConfig( c4 );
        cm.registerConfig( c3 );

        //inserting group before page is existing
        cm.addUiGroup( g2 );
        cm.addUiGroup( g1 );
        cm.addUiPage( p );

        c1.setValue( "config1" );
        Assert.assertEquals( "config1", cm.getConfig( "conf.text.1" ).getValue() );
        Assert.assertEquals( 1, cm.getConfig( "double" ).getWebUiGroup() );

        System.out.println( cm.getPage(1) );

    }

}
