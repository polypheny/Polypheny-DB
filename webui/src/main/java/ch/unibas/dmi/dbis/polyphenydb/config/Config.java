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

package ch.unibas.dmi.dbis.polyphenydb.config;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

//todo missing fields
/** configuration object that can be accessed and altered via the ConfigManager */
public abstract class Config<T> {
    /** unique key */
    private String key;
    //private T value;
    private String description;
    private boolean requiresRestart = false;
    //ConfigValidator validationMethod;
    private String callWhenChanged;
    private WebUiValidator[] webUiValidators;
    private WebUiFormType webUiFormType;
    /** id of the WebUiGroup it should be displayed in */
    private Integer webUiGroup;
    /** id of the WebUiPage it should be displayed in */
    private Integer webUiOrder;

    /** for gson */
    private String configType;

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

    /** override Config c1 with Config c2 by c1.override(c2). c1 gets attributes of c2 if they are set in c2 but not in c1
     * @param in other config that sould ovveride this config */
    public Config override ( Config in ) {
        if ( this.getClass() != in.getClass() ) {
            System.err.println( "cannot override config of type "+this.getClass().toString()+" with config of type "+in.getClass().toString() );
            return this;//todo or throw error
        }
        if ( in.key != null ) this.key = in.key;
        //if ( in.value != null ) this.value = in.value;
        if( in.getValue() != null ) this.setValue( (T) in.getValue());
        if ( in.description != null ) this.description = in.description;
        if ( in.requiresRestart ) this.requiresRestart = true;
        //todo override validationMethod
        //if ( in.validationMethod != null ) this.validationMethod = in.validationMethod;
        if ( in.callWhenChanged != null ) this.callWhenChanged = in.callWhenChanged;
        //todo webUiValidators
        if ( in.webUiFormType != null ) this.webUiFormType = in.webUiFormType;
        if ( in.webUiGroup != null ) this.webUiGroup = in.webUiGroup;
        if ( in.webUiOrder != null ) this.webUiOrder = in.webUiOrder;
        return this;
    }

    /** sets requiresRestart to true (is false by default) */
    public Config requiresRestart() {
        this.requiresRestart = true;
        return this;
    }

    /** set Ui information
     * @param webUiGroup id of webUiGroup
     * @param type type, e.g. text or number
     * */
    public Config withUi ( int webUiGroup, WebUiFormType type ) {
        this.webUiGroup = webUiGroup;
        this.webUiFormType = type;
        return this;
    }

    /** validators for the WebUi */
    public Config withWebUiValidation(WebUiValidator... validations) {
        this.webUiValidators = validations;
        return this;
    }

    // set anonymous validation method for this config
    //public abstract Config withJavaValidation (ConfigValidator c);

    /** returns Config as json */
    public String toString() {

        // https://stackoverflow.com/questions/15736654/how-to-handle-deserializing-with-polymorphism
        /*RuntimeTypeAdapterFactory<Config> runtimeTypeAdapterFactory = RuntimeTypeAdapterFactory.of(Config.class, "configType")
                .registerSubtype( ConfigInteger.class, "Integer" )
                .registerSubtype( ConfigNumber.class, "Number" )
                .registerSubtype( ConfigString.class, "String" );*/

        // SuperclassExclusionStrategy: https://stackoverflow.com/questions/16476513/class-a-declares-multiple-json-fields

        Gson gson = new GsonBuilder()
                //.registerTypeAdapterFactory( runtimeTypeAdapterFactory )
                //.addDeserializationExclusionStrategy( new SuperclassExclusionStrategy() )
                //.addSerializationExclusionStrategy( new SuperclassExclusionStrategy() )
                .setPrettyPrinting()
                .create();

        //Gson gson = new Gson();
        return gson.toJson( this );
    }

    /** get config value in type T */
    public abstract T getValue();

    //todo what if cast not possible (wrong incoming type)
    /** set the value of the config */
    /*public void setValue( Object value ){
        this.value = (T) value;
    }*/
    public abstract void setValue( T value );

    /** get the key of the config */
    public String getKey() {
        return this.key;
    }

    public String getConfigType() {
        return configType;
    }

    /** get the WebUiGroup id */
    public int getWebUiGroup() {
        return webUiGroup;
    }

    public void setConfigType( String configType ) {
        this.configType = configType;
    }

}
