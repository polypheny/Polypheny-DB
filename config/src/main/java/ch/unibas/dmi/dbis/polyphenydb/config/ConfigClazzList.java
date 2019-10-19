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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.ConfigException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.reflections.Reflections;


public class ConfigClazzList extends Config {

    private final Set<Class> classes;
    private final List<Class> value;


    public ConfigClazzList( final String key, final Class superClass ) {
        super( key );
        Reflections reflections = new Reflections( "ch.unibas.dmi.dbis.polyphenydb" );
        //noinspection unchecked
        classes = ImmutableSet.copyOf( reflections.getSubTypesOf( superClass ) );
        this.value = new ArrayList<>();
    }


    public ConfigClazzList( final String key, final Class superClass, final List<Class> defaultValue ) {
        this( key, superClass );
        setClazzList( defaultValue );
    }


    @Override
    public Set<Class> getClazzes() {
        return classes;
    }


    @Override
    public List<Class> getClazzList() {
        return ImmutableList.copyOf( value );
    }


    @Override
    public boolean setClazzList( final List<Class> value ) {
        if ( classes.containsAll( value ) ) {
            if ( validate( value ) ) {
                this.value.clear();
                this.value.addAll( value );
                notifyConfigListeners();
                return true;
            } else {
                return false;
            }
        } else {
            throw new ConfigRuntimeException( "This list contains at least one class that does not implement the specified super class!" );
        }
    }


    @Override
    public boolean addClazz( final Class value ) {
        if ( classes.contains( value ) ) {
            if ( validate( value ) ) {
                this.value.add( value );
                notifyConfigListeners();
                return true;
            } else {
                return false;
            }
        } else {
            throw new ConfigRuntimeException( "This class does not implement the specified super class!" );
        }
    }


    @Override
    public boolean removeClazz( final Class value ) {
        boolean b = this.value.remove( value );
        notifyConfigListeners();
        return b;
    }


    @Override
    void setValueFromFile( final com.typesafe.config.Config conf ) {
        final List<String> value;
        try {
            value = conf.getStringList( this.getKey() ); // read value from config file
            this.value.clear();
            for ( String v : value ) {
                addClazz( getByString( v ) );
            }
        } catch ( ConfigException.Missing e ) {
            // This should have been checked before!
            throw new ConfigRuntimeException( "No config with this key found in the configuration file." );
        } catch ( ConfigException.WrongType e ) {
            throw new ConfigRuntimeException( "The value in the config file has a type which is incompatible with this config element." );
        }

    }


    private Class getByString( String str ) throws ConfigRuntimeException {
        for ( Class c : classes ) {
            if ( str.equalsIgnoreCase( c.getCanonicalName() ) ) {
                return c;
            }
        }
        throw new ConfigRuntimeException( "No class with name \"" + str + "\" found in the set of valid classes." );
    }

}
