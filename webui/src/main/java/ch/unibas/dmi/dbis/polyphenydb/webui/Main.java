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

package ch.unibas.dmi.dbis.polyphenydb.webui;


import ch.unibas.dmi.dbis.polyphenydb.config.ConfigInteger;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigManager;
import ch.unibas.dmi.dbis.polyphenydb.config.WebUiGroup;
import ch.unibas.dmi.dbis.polyphenydb.config.WebUiPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger( Main.class );

    public static void main( final String[] args ) {
        if( args.length < 4 ) {
            LOGGER.error( "Missing command-line arguments. Please provide the following information:\n"
                    + "java Server <host> <port> <database> <user> <password>\n"
                    + "e.g. java Server localhost 8080 myDatabase root secret" );
            System.exit( 1 );
        }
        String host = args[0];
        int port = Integer.parseInt( args[1] );
        String dbName = args[2];
        String user = args[3];
        String pass = "";
        if ( args.length > 4 ) {
            pass = args[4];
        }

        Crud crud = new CrudPostgres( "org.postgresql.Driver", "jdbc:postgresql://", host, port, dbName, user, pass );

        ConfigManager cm = ConfigManager.getInstance();
        WebUiPage configPage = new WebUiPage( "ports", "ports", "Ports for the ConfigurationServer, InformationServer and the WebUi" );
        WebUiGroup portsGroup = new WebUiGroup( "portsGroup", "ports" ).withTitle( "ports" );
        cm.registerWebUiPage( configPage );
        cm.registerWebUiGroup( portsGroup );
        cm.registerConfig( new ConfigInteger( "configServer.port", "port of the ConfigServer", 8081 ).withUi( "portsGroup", 1 ) );
        cm.registerConfig( new ConfigInteger( "informationServer.port", "port of the InformationServer", 8082 ).withUi( "portsGroup", 2 ) );
        cm.registerConfig( new ConfigInteger( "webUI.port", "port of the webUI server", 8080 ).withUi( "portsGroup", 3 ) );

        //Spark.ignite: see https://stackoverflow.com/questions/41452156/multiple-spark-servers-in-a-single-jvm
        ConfigServer configServer = new ConfigServer( cm.getConfig( "configServer.port" ).getInt() );
        InformationServer informationServer = new InformationServer( cm.getConfig( "informationServer.port" ).getInt() );
        WebUiInterface webUIWebUiInterface = new WebUiInterface( null, null, cm.getConfig( "webUI.port" ).getInt(), crud );
    }

}
