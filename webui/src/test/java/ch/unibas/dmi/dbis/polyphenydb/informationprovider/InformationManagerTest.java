package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


import static org.junit.Assert.*;

import org.junit.Test;


public class InformationManagerTest {

    private InformationManager im;

    static {
        InformationManager im = InformationManager.getInstance();

        InformationPage p = new InformationPage( "page1", "Page 1" );
        InformationGroup g = new InformationGroup( "group1.1", "page1" );
        im.addPage( p );
        im.addGroup( g );

        Information i1 = new InformationHeader( "i.header", "group1.1", "Gruppe 1" );
        Information i2 = new InformationProgress( "i.progress", "group1.1", "progval", 30 );
        Information i4 = new InformationHtml( "i.html" , "group1.1", "<b>bold</b>");

        im.regsiterInformation( i1, i2, i4 );
    }

    public InformationManagerTest() {
        this.im = InformationManager.getInstance();
    }

    @Test
    public void getPage () {
        System.out.println( this.im.getPage( "page1" ) );
        //Gson gson = new Gson();
        //InformationPage p = gson.fromJson( this.im.getPage( "page1" ), InformationPage.class );
    }

    @Test
    public void getPageList () {
        System.out.println( this.im.getPageList() );
    }

}
