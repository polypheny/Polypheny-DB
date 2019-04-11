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


public class ConfigString extends ConfigScalar {

    private String value;
    private ConfigValidator validationMethod; // TODO MV: Why here???


    public ConfigString( final String key, final String value ) {
        super( key );
        this.value = value;
    }


    public ConfigString( final String key, final String description, final String value ) {
        super( key, description );
        this.value = value;
    }


    @Override
    Object getObject() {
        return this.value;
    }


    @Override
    public void setObject( final Object o ) {
        /*if(o == null){
            this.value = null;
            notifyConfigListeners( this );
            return;
        }*/
        String s = o.toString();
        if ( validate( s ) ) {
            this.value = s;
            notifyConfigListeners();
        }
    }


    @Override
    public String getString() {
        return this.value;
    }


    @Override
    public void setString( final String s ) {
        if ( validate( s ) ) {
            this.value = s;
            notifyConfigListeners();
        }
    }


    // TODO MV: Why here???
    private boolean validate( final String s ) {
        if ( this.validationMethod != null ) {
            if ( this.validationMethod.validate( s ) ) {
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


    // TODO MV: Why here???
    public ConfigString withJavaValidation( final ConfigValidator c ) {
        this.validationMethod = c;
        return this;
    }


    // TODO MV: Why here???
    public interface ConfigValidator {

        boolean validate( String a );
    }

}
