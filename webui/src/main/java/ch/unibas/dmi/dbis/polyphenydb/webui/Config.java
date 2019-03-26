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

//todo missing fields
public class Config<T> {
    private String key;
    private T value;
    private String description;
    private boolean requiresRestart = false;
    private String validationMethod;
    private String callWhenChanged;
    private String[] WebUiValidators;
    private String WebUiFormType;
    private String WebUiGroup;
    private int WebUiOrder;

    /**
     * @param key unique name for the configuration
     * */
    public Config ( String key ) {
        this.key = key;
    }

    /**
     * @param key unique name for the configuration
     * @param description description of the configuration
     * */
    public Config ( String key, String description ) {
        this.key = key;
        this.description = description;
    }

    /** sets requiresRestart to true (is false by default) */
    public Config requiresRestart() {
        this.requiresRestart = true;
        return this;
    }

    //todo
    public Config withValidation () {
        return this;
    }

    public String toString() {
        String out = "{";
        out += "key:"+key+",";
        out += "value:"+value.toString()+",";
        out += "description:"+description+",";
        out += "requiresResetart:"+requiresRestart+",";
        out += "validationMethod:"+validationMethod+",";
        //todo..
        out += "}";
        return out;
    }

    public T getValue() {
        return this.value;
    }

    //todo what if cast not possible (wrong incoming type)
    public void setValue( Object value ){
        this.value = (T) value;
    }

    public String getKey() {
        return this.key;
    }
}
