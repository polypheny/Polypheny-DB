package ch.unibas.dmi.dbis.polyphenydb.webui;

import ch.unibas.dmi.dbis.polyphenydb.config.*;
import org.junit.Assert;
import org.junit.Test;

public class ConfigManagerTest {

    @Test
    public void insertConfigTwice() {

        WebUiPage p = new WebUiPage( 1, "page1", "page1descr" );
        WebUiGroup g1 = new WebUiGroup( 1, 1 ).withTitle( "group1" );
        WebUiGroup g2 = new WebUiGroup( 2, 1 ).withDescription( "group2" );
        ConfigString c1 = new ConfigString("conf.text.1").withUi( 1, WebUiFormType.TEXT );
        ConfigString c2 = new ConfigString("conf.text.2").withUi( 1, WebUiFormType.TEXT );

        ConfigInteger c3 = new ConfigInteger( "double" );
        ConfigNumber c4 = new ConfigNumber( "double" ).withUi( 1, WebUiFormType.NUMBER );

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
        c2.setValue( "config2" );
        c3.setValue( 123 );
        c4.setValue( 789 );
        Assert.assertEquals( "config1", cm.getConfig( "conf.text.1" ).getValue() );
        Assert.assertEquals( 1, cm.getConfig( "double" ).getWebUiGroup() );

        System.out.println( cm.getPage(1) );

    }

}
