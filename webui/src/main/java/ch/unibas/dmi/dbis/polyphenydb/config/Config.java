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
public abstract class Config {

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


    /**
     * When you change a Config with a method like setInt() and the field validationMethod is set,
     * new value will only be set if the validation (ConfigValidator.validate()) returns true.
     */
    private ConfigValidator validationMethod;


    private String callWhenChanged;

    /**
     * Name of the validation method to use in the web ui
     * this field is parsed to Json by Gson
     */
    private WebUiValidator[] webUiValidators;

    /**
     * Form type to use in the web ui for this config element
     */
    WebUiFormType webUiFormType;

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
     *  If isObservable is false, listeners will not be notified when this Config changes.
     *  Needed for ConfigArray and ConfigTable: You get only one notification and not one for every element in them.
     */
    private boolean isObservable = true;

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
     * Allows to set requiresRestart. Is false by default.
     */
    public Config setRequiresRestart( final boolean requiresRestart ) {
        this.requiresRestart = requiresRestart;
        return this;
    }


    public boolean getRequiresRestart() {
        return this.requiresRestart;
    }


    /**
     * set Ui information
     *
     * @param webUiGroup id of webUiGroup
     */
    public Config withUi( String webUiGroup ) {
        if ( this.webUiFormType == null ) {
            throw new ConfigRuntimeException( "Config of type " + getClass().getSimpleName() + " cannot be rendered in the UI" );
        }
        this.webUiGroup = webUiGroup;
        return this;
    }


    /**
     * validators for the WebUi
     */
    public Config withWebUiValidation( final WebUiValidator... validations ) {
        this.webUiValidators = validations;
        return this;
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


    /**
     * Get the Long-Array representation of the configuration value.
     *
     * @return Configuration value as long[]
     */
    public long[] getLongArray() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a long[]!" );
    }


    public void setLongArray( final long[] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type long[] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the BigDecimal-Array representation of the configuration value.
     *
     * @return Configuration value as long[]
     */
    public BigDecimal[] getDecimalArray() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a BigDecimal[]!" );
    }


    public void setDecimalArray( final BigDecimal[] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type BigDecimal[] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the boolean-Array representation of the configuration value.
     *
     * @return Configuration value as boolean[]
     */
    public boolean[] getBooleanArray() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a boolean[]!" );
    }


    public void setBooleanArray( final boolean[] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type boolean[] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the String-Array representation of the configuration value.
     *
     * @return Configuration value as String[]
     */
    public String[] getStringArray() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a String[]!" );
    }


    public void setStringArray( final String[] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type String[] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


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


    /**
     * Get the Long-Table representation of the configuration value.
     *
     * @return Configuration value as long[][]
     */
    public long[][] getLongTable() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a long[][]!" );
    }


    public void setLongTable( final long[][] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type long[][] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the BigDecimal-Table representation of the configuration value.
     *
     * @return Configuration value as BigDecimal[][]
     */
    public BigDecimal[][] getDecimalTable() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a BigDecimal[][]!" );
    }


    public void setDecimalTable( final BigDecimal[][] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type BigDecimal[][] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the String-Table representation of the configuration value.
     *
     * @return Configuration value as String[][]
     */
    public String[][] getStringTable() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a String[][]!" );
    }


    public void setStringTable( final String[][] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type String[][] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Boolean-Table representation of the configuration value.
     *
     * @return Configuration value as booelan[][]
     */
    public boolean[][] getBooleanTable() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a boolean[][]!" );
    }


    public void setBooleanTable( final boolean[][] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type boolean[][] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


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
     * Needed for ConfigArray and ConfigTable. Their elements should not trigger a notification,
     * you only want to be notified once when the ConfigArray or ConfigTable changes.
     */
    Config isObservable ( boolean b) {
        this.isObservable = b;
        return this;
    }


    /**
     * Add an observer for this config element.
     * Configs from ConfigArray and ConfigTable (having isObservable = false) will be skipped
     *
     * @param listener Observer to add
     * @return Config
     */
    public Config addObserver( final ConfigListener listener ) {
        //don't observe if it is an element of ConfigArray or ConfigTable
        if (!isObservable) return this;
        if ( !this.listeners.contains( listener ) ) {
            this.listeners.add( listener );
        }
        return this;
    }


    /**
     * Remove an observer from this config element.
     *
     * @param listener Observer to remove
     * @return Config
     */
    public Config removeObserver( final ConfigListener listener ) {
        this.listeners.remove( listener );
        return this;
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


    boolean validate( final Object i ) {
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


    public Config withJavaValidation( final ConfigValidator c ) {
        this.validationMethod = c;
        return this;
    }


    /**
     * The observers of a Configuration object need to implement the method
     * onConfigChange() to define what needs to happen when this Configuration changes. The parameter "Config c" provides
     * the changed Config.
     * The method restart() can be implemented to define what will happen, when a Config changes, that requires a restart.
     */
    public interface ConfigListener {

        void onConfigChange( Config c );

        void restart( Config c );
    }

    /**
     * Interface needs to be implemented if you want to validate a value from a setter, before writing it to the Config
     * e.g. your ConfigValidator could enforce that an ConfigInteger accepts only an Integer < 10
     */
    public interface ConfigValidator {

        boolean validate( Object a );
    }

}
