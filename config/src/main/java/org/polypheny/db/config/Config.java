/*
 * Copyright 2019-2022 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.config;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.config.exception.ConfigRuntimeException;


/**
 * Configuration object that can be accessed and altered via the ConfigManager.
 */
public abstract class Config {

    /**
     * Unique key of this config.
     */
    private final String key;

    /**
     * Description of this configuration element.
     */
    private String description;

    /**
     * Indicated weather applying changes to this configuration element requires a restart of Polypheny-DB.
     */
    private boolean requiresRestart = false;

    /**
     * When you change a Config with a method like setInt() and the field validationMethod is set, new value will only be set
     * if the validation (ConfigValidator.validate()) returns true.
     */
    private ConfigValidator validationMethod;

    /**
     * Name of the validation method to use in the web ui this field is parsed to Json by Gson.
     */
    @SuppressWarnings({ "FieldCanBeLocal", "unused" })
    private WebUiValidator[] webUiValidators;

    /**
     * Form type to use in the web ui for this config element.
     */
    WebUiFormType webUiFormType;

    /**
     * ID of the WebUiGroup it should be displayed in.
     */
    private String webUiGroup;

    /**
     * Required by GSON.
     * Configs with a lower order will be rendered first.
     */
    @SuppressWarnings({ "FieldCanBeLocal", "unused" })
    private int webUiOrder;

    /**
     * Type of configuration element. Required for GSON.
     */
    @SuppressWarnings({ "FieldCanBeLocal", "unused" })
    private final String configType;

    /**
     * If isObservable is false, listeners will not be notified when this Config changes.
     * Needed for ConfigArray and ConfigTable: You get only one notification and not one for every element in them.
     */
    private boolean isObservable = true;

    /**
     * List of observers.
     * Field is transient, so it will not be (de)serialized by Gson and produce a "Unable to invoke no-args constructor" error.
     */
    private transient final Map<Integer, ConfigListener> listeners = new HashMap<>();


    /**
     * Constructor
     *
     * @param key Unique name for the configuration element.
     */
    protected Config( final String key ) {
        this( key, "" );
    }


    /**
     * Constructor
     *
     * @param key Unique name for the configuration element.
     * @param description Description of the configuration element.
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
     * Set Web UI information.
     *
     * @param webUiGroup ID of Web UI Group this Config should be placed in.
     */
    public Config withUi( final String webUiGroup ) {
        if ( this.webUiFormType == null ) {
            throw new ConfigRuntimeException( "Config of type " + getClass().getSimpleName() + " cannot be rendered in the UI" );
        }
        this.webUiGroup = webUiGroup;
        return this;
    }


    /**
     * Set Web UI information.
     * The ordering is happening in the Web UI.
     *
     * @param webUiGroup ID of Web UI Group this Config should be placed in.
     * @param order Placement of this config within the Web UI group. Configs with lower order will be rendered first.
     */
    public Config withUi( final String webUiGroup, final int order ) {
        if ( this.webUiFormType == null ) {
            throw new ConfigRuntimeException( "Config of type " + getClass().getSimpleName() + " cannot be rendered in the UI" );
        }
        this.webUiGroup = webUiGroup;
        this.webUiOrder = order;
        return this;
    }


    /**
     * Validators for the Web UI
     */
    public Config withWebUiValidation( final WebUiValidator... validations ) {
        this.webUiValidators = validations;
        return this;
    }


    /**
     * Get JSON representation of this configuration element.
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
     * @return Configuration value as String.
     * @throws ConfigRuntimeException If config value can not be converted into a String representation.
     */
    public String getString() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a String!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config.
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as String.
     */
    public boolean setString( final String value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type String on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Boolean representation of the configuration value.
     *
     * @return Configuration value as boolean.
     * @throws ConfigRuntimeException If config value can not be converted into a String representation.
     */
    public boolean getBoolean() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a boolean!" );
    }


    /**
     * Retrieves the current value as a plain object to be compared to other Objects.
     * NOTE: This method shall only be used for comparison reasons.
     *
     * If you want to retrieve the actual value use the respective get{DataType} functions.
     *
     * @return plain object value of config object
     */
    public abstract Object getPlainValueObject();


    /**
     * Retrieves the defaultValue that it is internally configured by the system.
     * If you want to reset it to the configured defaultValue use {@link #resetToDefault()}.
     *
     * @return defaultValue of config object
     */
    public abstract Object getDefaultValue();


    /**
     * Checks if the currently set config value, is equal to the system configured default.
     * If you want to reset it to the configured defaultValue use {@link #resetToDefault()}.
     * To change the systems default value you can use: {@link #changeDefaultValue(Object)}.
     *
     * @return true if it is set to default, false if it deviates
     */
    public abstract boolean isDefault();


    /**
     * Restores the current value to the system configured default value.
     *
     * To obtain the system configured defaultValue use {@link #getDefaultValue()}.
     * If you want to check if the current value deviates from default use: {@link #isDefault()}.
     */
    public abstract void resetToDefault();


    /**
     * Lets you change the internally configured system default Value to a new value.
     *
     * If you simply want to reset the current value to the default use: {@link #resetToDefault()}.
     */
    public void changeDefaultValue( Object newDefaultValue ) {
        throw new ConfigRuntimeException( "Change of defaultValue is not yet supported for type: " + this.getClass() );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as Boolean.
     */
    public boolean setBoolean( final boolean value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type boolean on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Integer representation of the configuration value.
     *
     * @return Configuration value as int
     * @throws ConfigRuntimeException If config value can not be converted into an Integer representation.
     */
    public int getInt() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a int!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as Integer.
     */
    public boolean setInt( final int value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type int on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Long representation of the configuration value.
     *
     * @return Configuration value as long
     * @throws ConfigRuntimeException If config value can not be converted into a Long representation.
     */
    public long getLong() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a long!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as Long.
     */
    public boolean setLong( final long value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type long on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Double representation of the configuration value.
     *
     * @return Configuration value as double
     * @throws ConfigRuntimeException If config value can not be converted into a Double representation.
     */
    public double getDouble() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a double!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as Double.
     */
    public boolean setDouble( final double value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type double on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Decimal representation of the configuration value.
     *
     * @return Configuration value as BigDecimal
     * @throws ConfigRuntimeException If config value can not be converted into a BigDecimal representation.
     */
    public BigDecimal getDecimal() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a BigDecimal!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as BigDecimal.
     */
    public boolean setDecimal( final BigDecimal value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type BigDecimal on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get a set af valid enum values.
     *
     * @return List of enum values
     * @throws ConfigRuntimeException If config value can not be converted into a Enum value representation.
     */
    public Set<Enum> getEnumValues() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a set of Enum values!" );
    }


    /**
     * Get the current value of the enum type.
     *
     * @return List of enum values
     * @throws ConfigRuntimeException If config value can not be converted into a Enum value representation.
     */
    public Enum getEnum() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a Enum value!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as class.
     */
    public boolean setEnum( final Enum value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type Enum value on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the current value of this config
     *
     * @return List of enum values.
     * @throws ConfigRuntimeException If config value can not be converted into a list of enums.
     */
    public List<Enum> getEnumList() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a list of enums!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as list of enums.
     */
    public boolean setEnumList( final List<Enum> value ) {
        throw new ConfigRuntimeException( "Not possible to set a list of enmus on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Add the value to the list.
     *
     * @param value Value to add
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as list of enums.
     */
    public boolean addEnum( final Enum value ) {
        throw new ConfigRuntimeException( "Not possible add a enum to a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Remove the value from the list.
     *
     * @param value Value to remove
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as list of enums.
     */
    public boolean removeEnum( final Enum value ) {
        throw new ConfigRuntimeException( "Not possible remove a enum from a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the current value of this config
     *
     * @return List of String values
     * @throws ConfigRuntimeException If config value can not be converted into a list of Strings
     */
    public List<String> getStringList() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a String value!" );
    }


    public <T> List<T> getList( Class<T> type ) {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a " + type.getSimpleName() + " value!" );
    }


    /**
     * Get the current value of this config
     *
     * @return List of Integer values
     * @throws ConfigRuntimeException If config value can not be converted into a list of Integer
     */
    public List<Integer> getIntegerList() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a Enum value!" );
    }


    /**
     * Get the current value of this config
     *
     * @return List of Double values
     * @throws ConfigRuntimeException If config value can not be converted into a list of Double
     */
    public List<Double> getDoubleList() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a Enum value!" );
    }


    /**
     * Get the current value of this config
     *
     * @return List of Long values
     * @throws ConfigRuntimeException If config value can not be converted into a list of Long.
     */
    public List<Long> getStringLong() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a Enum value!" );
    }


    /**
     * Get the current value of this config
     *
     * @return List of BigDecimal values
     * @throws ConfigRuntimeException If config value can not be converted into a list of BigDecimal.
     */
    public List<BigDecimal> getDecimalList() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a Enum value!" );
    }


    /**
     * Get the current value of this config
     *
     * @return List of Boolean values
     * @throws ConfigRuntimeException If config value can not be converted into a list of Boolean.
     */
    public List<Boolean> getBooleanList() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a Enum value!" );
    }


    /**
     * Sets the value of a config list which holds ConfigObjects.
     *
     * @param values the raw values ( object type )
     * @param clazz the class, which the raw values belong to
     * @return if the parsing and setting was successful
     */
    public boolean setConfigObjectList( final List<Object> values, Class<? extends ConfigScalar> clazz ) {
        throw new ConfigRuntimeException( "Not possible to set a list on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    public void setList( List<ConfigScalar> values ) {
        throw new ConfigRuntimeException( "Not possible to set a list on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get a list af classes implementing the specified super type.
     *
     * @return Set of classes
     * @throws ConfigRuntimeException If config value can not be converted into a set of classes.
     */
    public Set<Class> getClazzes() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a set of classes!" );
    }


    /**
     * Get the current value of this config.
     *
     * @return The current class
     * @throws ConfigRuntimeException If config value can not be converted into a class.
     */
    public Class getClazz() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a class!" );
    }


    /**
     * Set the value from this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as class.
     */
    public boolean setClazz( final Class value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type Class on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the current value of this config.
     *
     * @return List of enum values
     * @throws ConfigRuntimeException If config value can not be converted into a list of classes.
     */
    public List<Class> getClazzList() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a list of classes!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as list of classes.
     */
    public boolean setClazzList( final List<Class> value ) {
        throw new ConfigRuntimeException( "Not possible to set a list of classes on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Add the value to the list.
     *
     * @param value Value to add
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as list of classes.
     */
    public boolean addClazz( final Class value ) {
        throw new ConfigRuntimeException( "Not possible add a class on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Remove the value to the list.
     *
     * @param value Value to remove
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as list of classes.
     */
    public boolean removeClazz( final Class value ) {
        throw new ConfigRuntimeException( "Not possible remove a class on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }

    // ----- Arrays ------


    /**
     * Get the Integer-Array representation of the configuration value.
     *
     * @return Configuration value as int[]
     * @throws ConfigRuntimeException If config value can not be represented as an Integer-Array.
     */
    public int[] getIntArray() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into an int[]!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as Integer-Array.
     */
    public boolean setIntArray( final int[] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type int[] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Double-Array representation of the configuration value.
     *
     * @return Configuration value as double[]
     * @throws ConfigRuntimeException If config value can not be represented as a Double-Array.
     */
    public double[] getDoubleArray() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a double[]!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as Double-Array.
     */
    public boolean setDoubleArray( final double[] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type double[] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Long-Array representation of the configuration value.
     *
     * @return Configuration value as long[]
     * @throws ConfigRuntimeException If config value can not be represented as a Long-Array.
     */
    public long[] getLongArray() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a long[]!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as Long-Array.
     */
    public boolean setLongArray( final long[] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type long[] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the BigDecimal-Array representation of the configuration value.
     *
     * @return Configuration value as BigDecimal[]
     * @throws ConfigRuntimeException If config value can not be represented as a BigDecimal-Array.
     */
    public BigDecimal[] getDecimalArray() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a BigDecimal[]!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as BigDecimal-Array.
     */
    public boolean setDecimalArray( final BigDecimal[] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type BigDecimal[] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the boolean-Array representation of the configuration value.
     *
     * @return Configuration value as boolean[]
     * @throws ConfigRuntimeException If config value can not be represented as a Boolean-Array.
     */
    public boolean[] getBooleanArray() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a boolean[]!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as Boolean-Array.
     */
    public boolean setBooleanArray( final boolean[] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type boolean[] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the String-Array representation of the configuration value.
     *
     * @return Configuration value as String[]
     * @throws ConfigRuntimeException If config value can not be represented as a String-Array.
     */
    public String[] getStringArray() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a String[]!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as String-Array.
     */
    public boolean setStringArray( final String[] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type String[] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }

    // ----- Tables -----


    /**
     * Get the Integer-Table (Matrix) representation of the configuration value.
     *
     * @return Configuration value as int[][]
     * @throws ConfigRuntimeException If config value can not be represented as an Integer-Table.
     */
    public int[][] getIntTable() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a int[][]!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as Integer-Table.
     */
    public boolean setIntTable( final int[][] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type int[][] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Double-Table (Matrix) representation of the configuration value.
     *
     * @return Configuration value as double[][]
     * @throws ConfigRuntimeException If config value can not be represented as a Double-Table.
     */
    public double[][] getDoubleTable() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a double[][]!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as Double-Table.
     */
    public boolean setDoubleTable( final double[][] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type double[][] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Long-Table (Matrix) representation of the configuration value.
     *
     * @return Configuration value as long[][]
     * @throws ConfigRuntimeException If config value can not be represented as a Long-Table.
     */
    public long[][] getLongTable() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a long[][]!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as Long-Table.
     */
    public boolean setLongTable( final long[][] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type long[][] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the BigDecimal-Table (Matrix) representation of the configuration value.
     *
     * @return Configuration value as BigDecimal[][]
     * @throws ConfigRuntimeException If config value can not be represented as a BigDecimal-Table.
     */
    public BigDecimal[][] getDecimalTable() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a BigDecimal[][]!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as BigDecimal-Table.
     */
    public boolean setDecimalTable( final BigDecimal[][] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type BigDecimal[][] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the String-Table (Matrix) representation of the configuration value.
     *
     * @return Configuration value as String[][]
     * @throws ConfigRuntimeException If config value can not be represented as a String-Table.
     */
    public String[][] getStringTable() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a String[][]!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as String-Table.
     */
    public boolean setStringTable( final String[][] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type String[][] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the Boolean-Table (Matrix) representation of the configuration value.
     *
     * @return Configuration value as boolean[][]
     * @throws ConfigRuntimeException If config value can not be represented as a Boolean-Table.
     */
    public boolean[][] getBooleanTable() {
        throw new ConfigRuntimeException( "Configuration of type " + this.getClass().getSimpleName() + " cannot be converted into a boolean[][]!" );
    }


    /**
     * Set the value of this config.
     *
     * @param value New value for this config
     * @throws ConfigRuntimeException If this type of config is incompatible with a value represented as Boolean-Table.
     */
    public boolean setBooleanTable( final boolean[][] value ) {
        throw new ConfigRuntimeException( "Not possible to set a value of type boolean[][] on a configuration element of type " + this.getClass().getSimpleName() + "!" );
    }


    /**
     * Get the key of this config element
     *
     * @return Key as String
     */
    public String getKey() {
        return this.key;
    }


    /**
     * Type of configuration element
     *
     * @return The type of this configuration element as string
     */
    public String getConfigType() {
        return this.getClass().getSimpleName();
    }


    /**
     * Get the WebUiGroup, this configuration element belongs to,
     *
     * @return ID of the WebUiGroup
     */
    public String getWebUiGroup() {
        return webUiGroup;
    }


    /**
     * Needed for ConfigArray and ConfigTable. Their elements should not trigger a notification, you only want to be notified once when the ConfigArray or ConfigTable changes.
     */
    Config isObservable( final boolean b ) {
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
        if ( !isObservable ) {
            return this;
        }
        this.listeners.put( listener.hashCode(), listener );
        return this;
    }


    /**
     * Remove an observer from this config element.
     *
     * @param listener Observer to remove
     * @return Config
     */
    public Config removeObserver( final ConfigListener listener ) {
        this.listeners.remove( listener.hashCode() );
        return this;
    }


    /**
     * Notify observers
     */
    protected void notifyConfigListeners() {
        for ( ConfigListener listener : listeners.values() ) {
            listener.onConfigChange( this );
            if ( getRequiresRestart() ) {
                listener.restart( this );
            }
        }
    }


    boolean validate( final Object i ) {
        if ( this.validationMethod != null ) {
            return this.validationMethod.validate( i );
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
     * Get the description of this config.
     *
     * @return Description of the Config
     */
    public String getDescription() {
        return description;
    }


    /**
     * Set the current value to the one read from the config file.
     *
     * @param conf Config object from the HACON typesafe config library used to read the config files
     * @throws ConfigRuntimeException If value in the config file is incompatible with the type of the config element
     */
    abstract void setValueFromFile( com.typesafe.config.Config conf );


    /**
     * Parse the string and set as value.
     *
     * @param value The string to be parsed as value
     */
    public abstract boolean parseStringAndSetValue( String value );


    /**
     * Retrieve the template if set
     *
     * @return the template, which is a ConfigScalar
     */
    public Class<? extends ConfigScalar> getTemplateClass() {
        throw new ConfigRuntimeException( "No template was set for this configType" );
    }


    /**
     * The observers of a Configuration object need to implement the method onConfigChange() to define what needs to happen when this Configuration changes. The parameter "Config c" provides the changed Config.
     * The method restart() can be implemented to define what will happen, when a Config changes, that requires a restart.
     */
    public interface ConfigListener {

        void onConfigChange( Config c );

        void restart( Config c );

    }


    /**
     * Interface needs to be implemented if you want to validate a value from a setter, before writing it to the Config e.g. your ConfigValidator could enforce that an ConfigInteger accepts only an {@literal Integer < 10 }.
     */
    public interface ConfigValidator {

        boolean validate( Object a );

    }

}
