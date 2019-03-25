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

import java.util.Observable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.aeonbits.owner.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//todo singleton?
public class ConfigManager extends Observable {
    private ConcurrentMap<String, ConfigValue> config = new ConcurrentHashMap<String, ConfigValue>(  );
    private Logger log = LoggerFactory.getLogger( ConfigManager.class );

    boolean addConfig( String configName ) {
        if( validateConfig( configName )) {
            ConfigFactory.setProperty( "filename", configName );
            ConfigValue c = ConfigFactory.create( ConfigValue.class );
            this.config.put( configName,c );
            setChanged();
            notifyObservers( this.config );
            return true;
        } else {
            //log.info("did not add "+configName+" because too long");
            System.out.println( "did not add "+configName+" because too long" );
            return false;
        }
    }


    @Override
    public void notifyObservers( Object arg ) {
        super.notifyObservers( arg );
    }


    private boolean validateConfig ( String configName ) {
        return configName.length() <= 10;
    }
}
