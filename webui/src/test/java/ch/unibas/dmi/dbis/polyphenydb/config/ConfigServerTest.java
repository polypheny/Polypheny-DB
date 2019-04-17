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

package ch.unibas.dmi.dbis.polyphenydb.config;


import ch.unibas.dmi.dbis.polyphenydb.webui.ConfigServer;


public class ConfigServerTest {


    public static void main( String[] args ) {
        ConfigServer s = new ConfigServer();
        demoData( s );
    }


    /**
     * Test data
     */
    private static void demoData( ConfigServer s ) {
        System.out.println( "demoData()" );
        WebUiPage p = new WebUiPage( "p", "page 1", "page 1 descr." );
        WebUiPage p2 = new WebUiPage( "p2", "page 2", "page 2 description." ).withIcon( "fa fa-table" );
        WebUiGroup g1 = new WebUiGroup( "g1", "p", 2 ).withTitle( "group1" ).withDescription( "description of group1" );
        WebUiGroup g2 = new WebUiGroup( "g2", "p", 1 ).withTitle( "group2" ).withDescription( "group2" );
        Config c1 = new ConfigString( "server.text.1", "text1" ).withUi( "g1" ).withWebUiValidation( WebUiValidator.REQUIRED );
        Config c2 = new ConfigString( "server.email.2", "e@mail" ).withUi( "g1" ).withWebUiValidation( WebUiValidator.REQUIRED, WebUiValidator.EMAIL );

        //Config c3 = new ConfigInteger( "server.number", 3 );
        Config c4 = new ConfigInteger( "server.number", 4 ).withJavaValidation( a -> (int) a < 10 ).withUi( "g2" );
        Config c5 = new ConfigInteger( "server.number.2", 5 ).withUi( "g2", 1 );
        Config c6 = new ConfigBoolean( "server.boolean", false ).withUi( "g2" );

        ConfigManager cm = ConfigManager.getInstance();

        //inserting configs before groups and pages are existing
        cm.registerConfig( c1 );
        cm.registerConfig( c2 );
        cm.registerConfig( c4 );
        //cm.registerConfig( c3 );
        cm.registerConfig( c5 );
        cm.registerConfig( c6 );

        //throws error since it cannot be rendered in the UI
        //int[] arr = {1,2,3};
        //Config c7 = new ConfigArray( "config.array", arr ).withUi( "g2" );
        //cm.registerConfig( c7 );

        //inserting group before page is existing
        cm.registerWebUiGroup( g2 );
        cm.registerWebUiGroup( g1 );
        cm.registerWebUiPage( p );
        cm.registerWebUiPage( p2 );

        //c1.setString( "config1" );

        cm.observeAll( s );

        //timer for UI testing
        /*Timer timer = new Timer();
        timer.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                Random r = new Random();
                cm.getConfig( "server.number.2" ).setInt( r.nextInt(100) );
            }
        }, 10000, 10000 );*/

    }


}
