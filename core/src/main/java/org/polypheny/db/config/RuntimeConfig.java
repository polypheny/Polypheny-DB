/*
 * Copyright 2019-2020 The Polypheny Project
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


import java.math.BigDecimal;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.util.background.BackgroundTask;


public enum RuntimeConfig {

    APPROXIMATE_DISTINCT_COUNT(
            "runtime/approximateDistinctCount",
            "Whether approximate results from \"COUNT(DISTINCT ...)\" aggregate functions are acceptable.",
            false,
            ConfigType.BOOLEAN
    ), // Druid

    APPROXIMATE_TOP_N(
            "runtime/approximateTopN",
            "Whether approximate results from \"Top N\" queries (\"ORDER BY aggFun DESC LIMIT n\") are acceptable.",
            false,
            ConfigType.BOOLEAN
    ), // Druid

    APPROXIMATE_DECIMAL(
            "runtime/approximateDecimal",
            "Whether approximate results from aggregate functions on DECIMAL types are acceptable.",
            false,
            ConfigType.BOOLEAN
    ), // Druid

    NULL_EQUAL_TO_EMPTY(
            "runtime/nullEqualToEmpty",
            "Whether to treat empty strings as null for Druid Adapter.",
            true,
            ConfigType.BOOLEAN
    ), // Druid

    AUTO_TEMP(
            "runtime/autoTemp",
            "Whether to store query results in temporary tables.",
            false,
            ConfigType.BOOLEAN
    ),

    CASE_SENSITIVE(
            "runtime/caseSensitive",
            "Whether identifiers are matched case-sensitively.",
            false,
            ConfigType.BOOLEAN
    ),

    SPARK_ENGINE(
            "runtime/sparkEngine",
            "Specifies whether Spark should be used as the engine for processing that cannot be pushed to the source system. If false, Polypheny-DB generates code that implements the Enumerable interface.",
            false,
            ConfigType.BOOLEAN
    ),

    JDBC_PORT(
            "runtime/jdbcPort",
            "The port on which the JDBC server should listen.",
            20591,
            ConfigType.INTEGER
    ),

    CONFIG_SERVER_PORT(
            "runtime/configServerPort",
            "The port on which the config server should listen.",
            8081,
            ConfigType.INTEGER
    ),

    INFORMATION_SERVER_PORT(
            "runtime/informationServerPort",
            "The port on which the information server should listen.",
            8082,
            ConfigType.INTEGER
    ),

    WEBUI_SERVER_PORT(
            "runtime/webuiServerPort",
            "The port on which the web ui server should listen.",
            8080,
            ConfigType.INTEGER
    ),

    REL_WRITER_INSERT_FIELD_NAMES( "runtime/relWriterInsertFieldName",
            "If the rel writer should add the field names in brackets behind the ordinals in when printing query plans.",
            false,
            ConfigType.BOOLEAN ),

    QUERY_TIMEOUT( "runtime/queryTimeout",
            "Time after which queries are aborted. 0 means infinite.",
            0,
            ConfigType.INTEGER ),

    DEFAULT_INDEX_TYPE( "runtime/defaultIndexType",
            "Index type to use if no type is specified in the create index statement.",
            1,
            ConfigType.INTEGER ),

    DEFAULT_COLLATION( "runtime/defaultCollation",
            "Collation to use if no collation is specified",
            2,
            ConfigType.INTEGER ),

    GENERATED_NAME_PREFIX( "runtime/generatedNamePrefix",
            "Prefix for generated index, foreign key and constraint names.",
            "auto",
            ConfigType.STRING ),

    ADD_DEFAULT_VALUES_IN_INSERTS( "processing/addDefaultValuesInInserts",
            "Reorder columns and add default values in insert statements.",
            true,
            ConfigType.BOOLEAN,
            "parsingGroup" ),

    TRIM_UNUSED_FIELDS( "processing/trimUnusedFields",
            "Walks over a tree of relational expressions, replacing each RelNode with a 'slimmed down' relational expression that projects only the columns required by its consumer.",
            true,
            ConfigType.BOOLEAN,
            "planningGroup" ),

    DEBUG( "runtime/debug",
            "Print debugging output.",
            false,
            ConfigType.BOOLEAN ),

    JOIN_COMMUTE( "runtime/joinCommute",
            "Commute joins in planner.",
            false,
            ConfigType.BOOLEAN ),

    TWO_PC_MODE( "runtime/twoPcMode",
            "Use two-phase commit protocol for committing queries on data stores.",
            false,
            ConfigType.BOOLEAN,
            "runtimeGroup" ),
    DYNAMIC_QUERYING( "statistics/useDynamicQuerying",
            "Use statistics for query assistance.",
            true,
            ConfigType.BOOLEAN,
            "statisticSettingsGroup" ),
    ACTIVE_TRACKING( "statistics/activeTracking",
            "All transactions are tracked and statistics collected during execution.",
            true,
            ConfigType.BOOLEAN,
            "statisticSettingsGroup" ),
    PASSIVE_TRACKING( "statistics/passiveTracking",
            "Reevaluates statistics for all columns constantly, after a set time interval.",
            false,
            ConfigType.BOOLEAN,
            "statisticSettingsGroup" ),
    STATISTIC_BUFFER( "statistics/statisticColumnBuffer",
            "Number of buffered statistics e.g. for unique values.",
            5,
            ConfigType.INTEGER,
            "statisticSettingsGroup" ),
    UNIQUE_VALUES( "statistics/maxCharUniqueVal",
            "Maximum character of unique values",
            10,
            ConfigType.INTEGER,
            "statisticSettingsGroup" ),
    STATISTIC_RATE( "statistics/passiveTrackingRate",
            "Rate of passive tracking of statistics.",
            BackgroundTask.TaskSchedulingType.EVERY_THIRTY_SECONDS,
            ConfigType.ENUM ),
    UI_PAGE_SIZE( "ui/pageSize",
            "Number of rows per page in the data view.",
            10,
            ConfigType.INTEGER,
            "uiSettingsDataViewGroup" ),
    HUB_IMPORT_BATCH_SIZE( "hub/hubImportBatchSize",
            "Number of rows that should be inserted at a time when importing a dataset from Polypheny-Hub.",
            100,
            ConfigType.INTEGER,
            "uiSettingsDataViewGroup" ),
    SCHEMA_CACHING( "runtime/schemaCaching",
            "Cache polypheny-db schema",
            true,
            ConfigType.BOOLEAN );


    private final String key;
    private final String description;

    private final ConfigManager configManager = ConfigManager.getInstance();


    static {
        final ConfigManager configManager = ConfigManager.getInstance();

        // Query processing settings
        final WebUiPage processingPage = new WebUiPage(
                "processingPage",
                "Query Processing",
                "Settings influencing the query processing." );
        final WebUiGroup planningGroup = new WebUiGroup( "planningGroup", processingPage.getId() );
        final WebUiGroup parsingGroup = new WebUiGroup( "parsingGroup", processingPage.getId() );
        configManager.registerWebUiPage( processingPage );
        configManager.registerWebUiGroup( parsingGroup );
        configManager.registerWebUiGroup( planningGroup );

        // Runtime settings
        final WebUiPage runtimePage = new WebUiPage(
                "runtimePage",
                "Runtime Settings",
                "Core Settings" );
        final WebUiGroup runtimeGroup = new WebUiGroup( "runtimeGroup", runtimePage.getId() );
        configManager.registerWebUiPage( runtimePage );
        configManager.registerWebUiGroup( runtimeGroup );

        // Statistics and dynamic querying settings
        final WebUiPage queryStatisticsPage = new WebUiPage(
                "queryStatisticsPage",
                "Dynamic Querying",
                "Statistics Settings which can assists with building a query with dynamic assistance." );
        final WebUiGroup statisticSettingsGroup = new WebUiGroup( "statisticSettingsGroup", queryStatisticsPage.getId() ).withTitle( "Statistics Settings" );
        configManager.registerWebUiPage( queryStatisticsPage );
        configManager.registerWebUiGroup( statisticSettingsGroup );

        // UI specific setting
        final WebUiPage uiSettingsPage = new WebUiPage(
                "uiSettings",
                "Polypheny UI",
                "Settings for the user interface." );
        final WebUiGroup uiSettingsDataViewGroup = new WebUiGroup( "uiSettingsDataViewGroup", uiSettingsPage.getId() ).withTitle( "Data View" );
        configManager.registerWebUiPage( uiSettingsPage );
        configManager.registerWebUiGroup( uiSettingsDataViewGroup );
    }


    RuntimeConfig( final String key, final String description, final Object defaultValue, final ConfigType configType ) {
        this( key, description, defaultValue, configType, null );
    }


    RuntimeConfig( final String key, final String description, final Object defaultValue, final ConfigType configType, final String webUiGroup ) {
        this.key = key;
        this.description = description;

        final Config config;
        switch ( configType ) {
            case BOOLEAN:
                config = new ConfigBoolean( key, description, (boolean) defaultValue );
                break;

            case DECIMAL:
                config = new ConfigDecimal( key, description, (BigDecimal) defaultValue );
                break;

            case DOUBLE:
                config = new ConfigDouble( key, description, (double) defaultValue );
                break;

            case INTEGER:
                config = new ConfigInteger( key, description, (int) defaultValue );
                break;

            case LONG:
                config = new ConfigLong( key, description, (long) defaultValue );
                break;

            case STRING:
                config = new ConfigString( key, description, (String) defaultValue );
                break;

            case ENUM:
                config = new ConfigEnum( key, defaultValue.getClass(), (Enum) defaultValue );
                break;

            case BOOLEAN_TABLE:
                config = new ConfigTable( key, (boolean[][]) defaultValue );
                break;

            case DECIMAL_TABLE:
                config = new ConfigTable( key, (BigDecimal[][]) defaultValue );
                break;

            case DOUBLE_TABLE:
                config = new ConfigTable( key, (double[][]) defaultValue );
                break;

            case INTEGER_TABLE:
                config = new ConfigTable( key, (int[][]) defaultValue );
                break;

            case LONG_TABLE:
                config = new ConfigTable( key, (long[][]) defaultValue );
                break;

            case STRING_TABLE:
                config = new ConfigTable( key, (String[][]) defaultValue );
                break;

            case BOOLEAN_ARRAY:
                config = new ConfigArray( key, (boolean[]) defaultValue );
                break;

            case DECIMAL_ARRAY:
                config = new ConfigArray( key, (BigDecimal[]) defaultValue );
                break;

            case DOUBLE_ARRAY:
                config = new ConfigArray( key, (double[]) defaultValue );
                break;

            case INTEGER_ARRAY:
                config = new ConfigArray( key, (int[]) defaultValue );
                break;

            case LONG_ARRAY:
                config = new ConfigArray( key, (long[]) defaultValue );
                break;

            case STRING_ARRAY:
                config = new ConfigArray( key, (String[]) defaultValue );
                break;

            default:
                throw new RuntimeException( "Unknown config type: " + configType.name() );
        }
        configManager.registerConfig( config );
        if ( webUiGroup != null ) {
            config.withUi( webUiGroup );
        }
    }


    public boolean getBoolean() {
        return configManager.getConfig( key ).getBoolean();
    }


    public BigDecimal getDecimal() {
        return configManager.getConfig( key ).getDecimal();
    }


    public double getDouble() {
        return configManager.getConfig( key ).getDouble();
    }


    public Enum getEnum() {
        return configManager.getConfig( key ).getEnum();
    }


    public int getInteger() {
        return configManager.getConfig( key ).getInt();
    }


    public long getLong() {
        return configManager.getConfig( key ).getLong();
    }


    public String getString() {
        return configManager.getConfig( key ).getString();
    }

    // TODO: Add methods for array and table


    public void setBoolean( final boolean value ) {
        configManager.getConfig( key ).setBoolean( value );
    }


    public void setDecimal( final BigDecimal value ) {
        configManager.getConfig( key ).setDecimal( value );
    }


    public void setDouble( final double value ) {
        configManager.getConfig( key ).setDouble( value );
    }


    public void setInteger( final int value ) {
        configManager.getConfig( key ).setInt( value );
    }


    public void setLong( final long value ) {
        configManager.getConfig( key ).setLong( value );
    }


    public void setString( final String value ) {
        configManager.getConfig( key ).setString( value );
    }


    public void addObserver( final ConfigListener listener ) {
        configManager.getConfig( key ).addObserver( listener );
    }


    public enum ConfigType {
        BOOLEAN, DECIMAL, DOUBLE, INTEGER, LONG, STRING, ENUM, BOOLEAN_TABLE, DECIMAL_TABLE, DOUBLE_TABLE, INTEGER_TABLE, LONG_TABLE, STRING_TABLE, BOOLEAN_ARRAY, DECIMAL_ARRAY, DOUBLE_ARRAY, INTEGER_ARRAY, LONG_ARRAY, STRING_ARRAY
    }

}
