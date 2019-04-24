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


import com.google.gson.Gson;


public class ConfigInteger extends ConfigScalar {

    private int value;


    public ConfigInteger( final String key, final int value ) {
        super( key );
        this.webUiFormType = WebUiFormType.NUMBER;
        this.value = value;
    }


    public ConfigInteger( final String key, final String description, final int value ) {
        super( key, description );
        this.webUiFormType = WebUiFormType.NUMBER;
        this.value = value;
    }


    @Override
    public int getInt() {
        return this.value;
    }


    @Override
    public boolean setInt( final int value ) {
        if ( validate( value ) ) {
            this.value = value;
            notifyConfigListeners();
            return true;
        } else {
            return false;
        }
    }


    @Override
    public long getLong() {
        return (long) this.value;
    }


    @Override
    public boolean setString( String value ) {
        Gson gson = new Gson();
        Integer i = gson.fromJson( value, Integer.class );
        return setInt( i );
    }

}
