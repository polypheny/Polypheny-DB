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


import ch.unibas.dmi.dbis.polyphenydb.config.exception.ConfigRuntimeException;
import java.util.Set;
import org.reflections.Reflections;


public class ConfigClazz extends Config {

    private Set<Class<?>> classes;
    private Class value;


    public ConfigClazz( final String key, final Class superClass, final Class defaultValue ) {
        super( key );
        Reflections reflections = new Reflections( "ch.unibas.dmi.dbis.polyphenydb" );
        classes = reflections.getSubTypesOf( superClass );
        this.value = defaultValue;
    }


    @Override
    public Set<Class<?>> getClazzes() {
        return classes;
    }


    @Override
    public Class getClazz() {
        return value;
    }


    @Override
    public boolean setClazz( final Class value ) {
        if ( classes.contains( value ) ) {
            if ( validate( value ) ) {
                this.value = value;
                notifyConfigListeners();
                return true;
            } else {
                return false;
            }
        } else {
            throw new ConfigRuntimeException( "This class does not implement the specified super class" );
        }
    }


    @Override
    void setValueFromFile( final com.typesafe.config.Config conf ) {
        throw new ConfigRuntimeException( "Reading class from config files is not supported yet." );
    }

}
