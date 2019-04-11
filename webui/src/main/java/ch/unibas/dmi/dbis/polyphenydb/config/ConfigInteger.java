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


public class ConfigInteger extends ConfigScalar {

    private int value;
    private ConfigValidator validationMethod; // TODO MV: Why is this defined here


    public ConfigInteger( final String key, final int value ) {
        super( key );
        this.value = value;
    }


    public ConfigInteger( final String key, final String description, final int value ) {
        super( key, description );
        this.value = value;
    }


    @Override
    public Object getObject() {
        return this.value;
    }


    @Override
    public void setObject( Object o ) {
        //todo or parseInt
        Integer i;
        try {
            Double d = (Double) o;
            i = d.intValue();
        } catch ( ClassCastException e ) {
            i = (int) o;
        }
        if ( validate( i ) ) {
            this.value = i;
            notifyConfigListeners();
        }
    }


    @Override
    public int getInt() {
        return this.value;
    }


    @Override
    public void setInt( final int value ) {
        if ( validate( value ) ) {
            this.value = value;
            notifyConfigListeners();
        }
    }


    @Override
    public long getLong() {
        return (long) this.value;
    }


    // TODO MV: Why here
    private boolean validate( final int i ) {
        if ( this.validationMethod != null ) {
            if ( this.validationMethod.validate( i ) ) {
                return true;
            } else {
                System.out.println( "Java validation: false." );
                return false;
            }
        } //else if (this.validationMethod == null ) {
        else {
            return true;
        }
    }


    // TODO MV: Why here
    public ConfigInteger withJavaValidation( final ConfigValidator c ) {
        this.validationMethod = c;
        return this;
    }


    // TODO MV: Why here
    public interface ConfigValidator {

        boolean validate( Integer a );
    }

}
