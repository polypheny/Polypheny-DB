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


import com.google.gson.Gson;


//todo missing fields
/** configuration object that can be accessed and altered via the ConfigManager */
public class Config<T> {
    /** unique key */
    private String key;
    private T value;
    private String description;
    private boolean requiresRestart = false;
    private String validationMethod;
    private String callWhenChanged;
    private WebUiValidator[] webUiValidators;
    private WebUiFormType webUiFormType;
    /** id of the WebUiGroup it should be displayed in */
    private Integer webUiGroup;
    /** id of the WebUiPage it should be displayed in */
    private Integer webUiOrder;

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

    /** override this with another config in
     * @param in config that sould ovveride this config */
    public Config<T> override ( Config<T> in ) {
        if ( this.getClass() != in.getClass() ) {
            System.err.println( "cannot override config of type "+this.getClass().toString()+" with config of type "+in.getClass().toString() );
            return this;//todo or throw error
        }
        if ( in.key != null ) this.key = in.key;
        if ( in.value != null ) this.value = in.value;
        if ( in.description != null ) this.description = in.description;
        if ( in.requiresRestart ) this.requiresRestart = true;
        if ( in.validationMethod != null ) this.validationMethod = in.validationMethod;
        if ( in.callWhenChanged != null ) this.callWhenChanged = in.callWhenChanged;
        //todo webUiValidators
        if ( in.webUiFormType != null ) this.webUiFormType = in.webUiFormType;
        if ( in.webUiGroup != null ) this.webUiGroup = in.webUiGroup;
        if ( in.webUiOrder != null ) this.webUiOrder = in.webUiOrder;
        return this;
    }

    /** sets requiresRestart to true (is false by default) */
    public Config<T> requiresRestart() {
        this.requiresRestart = true;
        return this;
    }

    /** set Ui information
     * @param webUiGroup id of webUiGroup
     * @param type type, e.g. text or number
     * */
    public Config<T> withUi ( int webUiGroup, WebUiFormType type ) {
        this.webUiGroup = webUiGroup;
        this.webUiFormType = type;
        return this;
    }

    /** validators for the WebUi */
    public Config<T> withValidation (WebUiValidator... validations) {
        this.webUiValidators = validations;
        return this;
    }

    /** returns Config as json */
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson( this );
    }

    /** get config value in type T */
    public T getValue() {
        return this.value;
    }

    //todo what if cast not possible (wrong incoming type)
    /** set the value of the config */
    public void setValue( Object value ){
        this.value = (T) value;
    }

    /** get the key of the config */
    public String getKey() {
        return this.key;
    }

    /** get the WebUiGroup id */
    public int getWebUiGroup() {
        return webUiGroup;
    }
}
/** type of the config for the WebUi to specify how it should be rendered in the UI (&lt;input type="text/number/etc."&gt;)
 * e.g. text or number */
enum WebUiFormType{
    TEXT("text"),
    NUMBER("number");

    private final String type;

    WebUiFormType( String t ) {
        this.type = t;
    }

    @Override
    public String toString() {
        return this.type;
    }
}
//todo add more
/** supported Angular form validators */
enum WebUiValidator{
    REQUIRED("required"),
    EMAIL("email");

    private final String validator;

    WebUiValidator(String s){
        this.validator = s;
    }

    @Override
    public String toString() {
        return this.validator;
    }

}