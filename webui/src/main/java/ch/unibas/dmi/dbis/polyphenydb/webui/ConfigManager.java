/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
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
 */

package ch.unibas.dmi.dbis.polyphenydb.webui;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

//todo observable
public class ConfigManager {

    private static ConfigManager instance;

    private static ConcurrentMap<String, Config> config;
    //private Logger log = LoggerFactory.getLogger( ConfigManager.class );

    private ConfigManager() {
        config = new ConcurrentHashMap<String, Config>();
    }

    public static ConfigManager getInstance () {
        if(instance == null) {
            instance = new ConfigManager();
        }
        return instance;
    }

    //todo what if already exists
    static boolean registerConfig( Config c) {
        if( validateConfig( c )) {
            config.put( c.getKey(),c );
            //setChanged();
            //notifyObservers( this.config );
            return true;
        } else {
            //log.info("did not add "+configName+" because too long");
            System.out.println( "did not add "+c.getKey()+" because keyname too long" );
            return false;
        }
    }

    /**
     * @param key (unique) key of the configuration
     * */
    static Object getObject ( String key) {
        return config.get( key ).getValue();
    }

    static int getInt ( String key ) {
        return (int) config.get( key ).getValue();
    }

    static String getString ( String key ) {
        return (String) config.get( key ).getValue();
    }

    static Config getConfig( String s ) {
        return config.get( s );
    }


    /*static boolean set( String s, Config c ) {
        if( config.get( s ) != null){
            config.put(s, c);
            return true;
        } else {
          return false;
        }
    }*/

    //todo throw exception if config does not exist
    static boolean set ( String s, Object v ) {
        if( config.get( s ) != null){
            config.get( s ).setValue( v );
            return true;
        } else {
            return false;
        }
    }

    private static boolean validateConfig ( Config c ) {
        return c.getKey().length() <= 100;
    }
}
