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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.math.BigDecimal;
import java.util.concurrent.ConcurrentLinkedQueue;

// todo missing fields


/**
 * Configuration object that can be accessed and altered via the ConfigManager
 */
public abstract class Config<T extends Config<T>> {

    /**
     * unique key
     */
    private final String key;

    /**
     * Description of this configuration element
     */
    private String description;

    /**
     * Indicated weather applying changes to this configuration element requires a restart of Polypheny-DB
     */
    private boolean requiresRestart = false;

    // TODO MV: Missing
    //ConfigValidator validationMethod;


    private String callWhenChanged;

    // TODO MV: Why is this field unused
    /**
     * Name of the validation method to use in the web ui
     */
    private WebUiValidator[] webUiValidators;

    /**
     * Form type to use in the web ui for this config element
     */
    private WebUiFormType webUiFormType;

    /**
     * id of the WebUiGroup it should be displayed in
     */
    private String webUiGroup;

    /**
     * id of the WebUiPage it should be displayed in
     */
    private Integer webUiOrder;

    /**
     * Type of configuration element. Required for GSON.
     */
    @SuppressWarnings("unused")
    private final String configType;


    /**
     * List of observers
     */
    private final ConcurrentLinkedQueue<ConfigListener> listeners = new ConcurrentLinkedQueue<>();


    /**
     * Constructor
     *
     * @param key Unique name for the configuration element
     */
    protected Config( final String key ) {
        this( key, "" );
    }


    /**
     * Constructor
     *
     * @param key Unique name for the configuration element
     * @param description Description of the configuration element
     */
    protected Config( final String key, final String description ) {
        this.key = key;
        this.description = description;
        configType = getConfigType();
    }


    /**
     * Override Config c1 with Config c2 by c1.override(c2). c1 gets attributes of c2 if they are set in c2 but not in c1
     *
     * @param in other config that should override this config
     */
    public T override( final Config in ) {
        if ( this.getClass() != in.getClass() ) {
            System.err.println( "Cannot override config of type " + this.getClass().toString() + " with config of type " + in.getClass().toString() );
            return (T) this;// todo or throw error
        }
        //if ( in.key != null ) {
        //    this.key = in.key;
        //}
        // If ( in.value != null ) this.value = in.value;
        if ( in.getObject() != null && !in.getObject().equals( this.getObject() ) ) {
            this.setObject( in.getObject() );
        }
        if ( in.description != null ) {
            this.description = in.description;
        }
        if ( in.requiresRestart ) {
            this.requiresRestart = true;
        }
        // todo override validationMethod
        // If ( in.validationMethod != null ) this.validationMethod = in.validationMethod;
        if ( in.callWhenChanged != null ) {
            this.callWhenChanged = in.callWhenChanged;
        }
        //todo webUiValidators
        if ( in.webUiFormType != null ) {
            this.webUiFormType = in.webUiFormType;
        }
        if ( in.webUiGroup != null ) {
            this.webUiGroup = in.webUiGroup;
        }
        if ( in.webUiOrder != null ) {
            this.webUiOrder = in.webUiOrder;
        }
        return (T) this;
    }


    /**
     * Allows to set requiresRestart. Is false by default.
     */
    public T setRequiresRestart( final boolean requiresRestart ) {
        this.requiresRestart = requiresRestart;
        return (T) this;
    }


    public boolean getRequiresRestart() {
        return this.requiresRestart;
    }


    /**
     * set Ui information
     *
     * @param webUiGroup id of webUiGroup
     * @param type type, e.g. text or number
     */
    public T withUi ( String webUiGroup, WebUiFormType type ) {
        this.webUiGroup = webUiGroup;
        this.webUiFormType = type;
        return (T) this;
    }


    /**
     * validators for the WebUi
     */
    public T withWebUiValidation( final WebUiValidator... validations ) {
        this.webUiValidators = validations;
        return (T) this;
    }

    // TODO MV:
    // set anonymous validation method for this config
    //public abstract Config withJavaValidation (ConfigValidator c);


    /**
     * Get JSON representation of this configuration element
     *
     * @return Config as JSON
     */
    public String toJson() {
        Gson gson = new GsonBuilder()
                .setPrettyPrinting()
                .create();
        return gson.toJson( this );
    }


    // TODO MV: ???
    Object getObject() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into an Object!" );
    }


    // TODO MV: ???
    abstract void setObject( final Object value );//needed in ConfigManager.override and ConfigManager.setConfigValue

    //  ----- Scalars -----


    /**
     * Get the String representation of the configuration value.
     *
     * @return Configuration value as String
     */
    public String getString() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a String!" );
    }


    public void setString( final String value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type String on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Boolean representation of the configuration value.
     *
     * @return Configuration value as boolean
     */
    public boolean getBoolean() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a boolean!" );
    }


    public void setBoolean( final boolean value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type boolean on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Integer representation of the configuration value.
     *
     * @return Configuration value as int
     */
    public int getInt() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a int!" );
    }


    public void setInt( final int value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type int on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Long representation of the configuration value.
     *
     * @return Configuration value as long
     */
    public long getLong() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a long!" );
    }


    public void setLong( final long value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type long on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Double representation of the configuration value.
     *
     * @return Configuration value as double
     */
    public double getDouble() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a double!" );
    }


    public void setDouble( final double value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type double on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Decimal representation of the configuration value.
     *
     * @return Configuration value as BigDecimal
     */
    public BigDecimal getDecimal() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a BigDecimal!" );
    }


    public void setDecimal( final BigDecimal value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type BigDecimal on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }

    // ----- Arrays ------


    /**
     * Get the Integer-Array representation of the configuration value.
     *
     * @return Configuration value as int[]
     */
    public int[] getIntArray() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into an int[]!" );
    }


    public void setIntArray( final int[] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type int[] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Double-Array representation of the configuration value.
     *
     * @return Configuration value as double[]
     */
    public double[] getDoubleArray() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a double[]!" );
    }


    public void setDoubleArray( final double[] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type double[] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }

    // TODO MV: Missing methods for String, boolean, long, decimal

    // ----- Tables -----


    /**
     * Get the Integer-Table representation of the configuration value.
     *
     * @return Configuration value as int[][]
     */
    public int[][] getIntTable() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a int[][]!" );
    }


    public void setIntTable( final int[][] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type int[][] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Double-Table representation of the configuration value.
     *
     * @return Configuration value as double[][]
     */
    public double[][] getDoubleTable() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a double[][]!" );
    }


    public void setDoubleTable( final double[][] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type double[][] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }

    // TODO MV: Missing methods for String, boolean, long, decimal

    // TODO MV: Use lombok


    /**
     * Get the key of this config element
     *
     * @return Key as String
     */
    public String getKey() {
        return this.key;
    }

    // TODO MV: Use lombok


    /**
     * Type of configuration element
     *
     * @return The type of this configuration element as string
     */
    public String getConfigType() {
        return this.getClass().getSimpleName();
    }

    // TODO MV: Use lombok


    /**
     * Get the WebUiGroup, this configuration element belongs to,
     *
     * @return ID of the WebUiGroup
     */
    public String getWebUiGroup() {
        return webUiGroup;
    }


    /**
     * Add an observer for this config element.
     *
     * @param listener Observer to add
     * @return Config
     */
    public T addObserver( final ConfigListener listener ) {
        if ( !this.listeners.contains( listener ) ) {
            this.listeners.add( listener );
        }
        return (T) this;
    }


    /**
     * Remove an observer from this config element.
     *
     * @param listener Observer to remove
     * @return Config
     */
    public T removeObserver( final ConfigListener listener ) {
        this.listeners.remove( listener );
        return (T) this;
    }


    /**
     * Notify observers
     */
    protected void notifyConfigListeners() {
        for ( ConfigListener listener : listeners ) {
            listener.onConfigChange( this );
            if ( getRequiresRestart() ) {
                listener.restart( this );
            }
        }
    }


    // TODO MV: JavaDoc
    public interface ConfigListener {

        void onConfigChange( Config c );

        void restart( Config c );
    }

}
