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


import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.util.background.BackgroundTask;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;


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

    CASE_SENSITIVE(
            "runtime/caseSensitive",
            "Whether identifiers are matched case-sensitively.",
            false,
            ConfigType.BOOLEAN
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

    REL_WRITER_INSERT_FIELD_NAMES(
            "runtime/relWriterInsertFieldName",
            "If the alg writer should add the field names in brackets behind the ordinals in when printing query plans.",
            false,
            ConfigType.BOOLEAN ),

    QUERY_TIMEOUT(
            "runtime/queryTimeout",
            "Time after which queries are aborted. 0 means infinite.",
            0,
            ConfigType.INTEGER,
            "processingExecutionGroup" ),

    DEFAULT_COLLATION(
            "runtime/defaultCollation",
            "Collation to use if no collation is specified",
            2,
            ConfigType.INTEGER ),

    GENERATED_NAME_PREFIX(
            "runtime/generatedNamePrefix",
            "Prefix for generated index, foreign key and constraint names.",
            "auto",
            ConfigType.STRING ),

    ADD_DEFAULT_VALUES_IN_INSERTS(
            "processing/addDefaultValuesInInserts",
            "Reorder columns and add default values in insert statements.",
            true,
            ConfigType.BOOLEAN,
            "parsingGroup" ),

    TRIM_UNUSED_FIELDS(
            "processing/trimUnusedFields",
            "Walks over a tree of relational expressions, replacing each {@link AlgNode} with a 'slimmed down' relational expression that projects only the columns required by its consumer.",
            true,
            ConfigType.BOOLEAN,
            "planningGroup" ),

    DEBUG(
            "runtime/debug",
            "Print debugging output.",
            false,
            ConfigType.BOOLEAN ),

    JOIN_COMMUTE(
            "runtime/joinCommute",
            "Commute joins in planner.",
            false,
            ConfigType.BOOLEAN,
            "planningGroup" ),

    VALIDATE_MM_CONTENT_TYPE(
            "validation/validateMultimediaContentType",
            "Validate multimedia data by checking its content-type.",
            true,
            ConfigType.BOOLEAN,
            "validationGroup"
    ),

    TWO_PC_MODE(
            "runtime/twoPcMode",
            "Use two-phase commit protocol for committing queries on data stores.",
            false,
            ConfigType.BOOLEAN ),
    // "processingExecutionGroup" ),

    DYNAMIC_QUERYING(
            "statistics/useDynamicQuerying",
            "Use statistics for query assistance.",
            true,
            ConfigType.BOOLEAN,
            "statisticSettingsGroup" ),

    STATISTICS_ON_STARTUP(
            "statistics/statisticsOnStartup",
            "Whether to build statistics for all stored data on system startup.",
            true,
            ConfigType.BOOLEAN,
            "statisticSettingsGroup" ),

    ACTIVE_TRACKING(
            "statistics/activeTracking",
            "All transactions are tracked and statistics collected during execution.",
            true,
            ConfigType.BOOLEAN,
            "statisticSettingsGroup" ),

    PASSIVE_TRACKING(
            "statistics/passiveTracking",
            "Reevaluates statistics for all columns constantly, after a set time interval.",
            false,
            ConfigType.BOOLEAN,
            "statisticSettingsGroup" ),

    STATISTIC_BUFFER(
            "statistics/statisticColumnBuffer",
            "Number of buffered statistics e.g. for unique values.",
            5,
            ConfigType.INTEGER,
            "statisticSettingsGroup" ),

    UNIQUE_VALUES(
            "statistics/maxCharUniqueVal",
            "Maximum character of unique values",
            10,
            ConfigType.INTEGER,
            "statisticSettingsGroup" ),

    STATISTIC_RATE(
            "statistics/passiveTrackingRate",
            "Rate of passive tracking of statistics.",
            BackgroundTask.TaskSchedulingType.EVERY_THIRTY_SECONDS_FIXED,
            ConfigType.ENUM ),

    MATERIALIZED_VIEW_LOOP(
            "materializedView/freshnessLoopRate",
            "Rate of freshness Loop for Materialized Views with update type interval.",
            TaskSchedulingType.EVERY_SECOND_FIXED,
            ConfigType.ENUM ),

    EXPLORE_BY_EXAMPLE_TO_SQL(
            "exploreByExample/classificationToSQL",
            "Build SQL query from classification.",
            true,
            ConfigType.BOOLEAN,
            "uiSettings" ),

    UI_PAGE_SIZE(
            "ui/pageSize",
            "Number of rows per page in the data view.",
            10,
            ConfigType.INTEGER,
            "uiSettingsDataViewGroup" ),

    UI_UPLOAD_SIZE_MB(
            "ui/uploadSizeMB",
            "Maximum size of a file upload for multimedia data in the UI, in MB. "
                    + "When creating a HSQLDB multimedia column, this size is applied as the max-size of the underlying HSQLDB BLOB column.",
            10_000,
            ConfigType.INTEGER,
            "uiSettingsDataViewGroup" ),

    UI_USE_HARDLINKS(
            "ui/useHardlinks",
            "Whether or not to use hardlinks for temporal files in the UI. If false, softlinks are used. This config has only an effect when one or multiple file stores are deployed. "
                    + "With hardlinks, the data you see is the correct data that was selected during the transaction. "
                    + "But with multiple file stores on different file systems, hardlinks won't work. "
                    + "In this case you can use softlinks, but you might see data that is more recent.",
            true,
            ConfigType.BOOLEAN,
            "uiSettingsDataViewGroup" ),

    HUB_IMPORT_BATCH_SIZE(
            "hub/hubImportBatchSize",
            "Number of rows that should be inserted at a time when importing a dataset from Polypheny-Hub.",
            1000,
            ConfigType.INTEGER,
            "uiSettingsDataViewGroup" ),

    SCHEMA_CACHING(
            "runtime/schemaCaching",
            "Cache polypheny-db schema",
            true,
            ConfigType.BOOLEAN ),

    QUERY_PLAN_CACHING(
            "runtime/queryPlanCaching",
            "Cache planned and optimized query plans.",
            true,
            ConfigType.BOOLEAN,
            "queryPlanCachingGroup" ),

    QUERY_PLAN_CACHING_DML(
            "runtime/queryPlanCachingDml",
            "Cache DML query plans.",
            true,
            ConfigType.BOOLEAN,
            "queryPlanCachingGroup" ),

    QUERY_PLAN_CACHING_SIZE(
            "runtime/queryPlanCachingSize",
            "Size of the query plan cache. If the limit is reached, the least recently used entry is removed.",
            1000,
            ConfigType.INTEGER,
            "queryPlanCachingGroup" ),

    IMPLEMENTATION_CACHING(
            "runtime/implementationCaching",
            "Cache implemented query plans.",
            true,
            ConfigType.BOOLEAN,
            "implementationCachingGroup" ),

    IMPLEMENTATION_CACHING_DML(
            "runtime/implementationCachingDml",
            "Cache implementation for DML queries.",
            true,
            ConfigType.BOOLEAN,
            "implementationCachingGroup" ),

    IMPLEMENTATION_CACHING_SIZE(
            "runtime/implementationCachingSize",
            "Size of the implementation cache. If the limit is reached, the least recently used entry is removed.",
            1000,
            ConfigType.INTEGER,
            "implementationCachingGroup" ),

    ROUTING_PLAN_CACHING(
            "runtime/routingPlanCaching",
            "Caching of routing plans.",
            true,
            ConfigType.BOOLEAN,
            "routingCache" ),

    ROUTING_PLAN_CACHING_SIZE(
            "runtime/routingPlanCachingSize",
            "Size of the routing plan cache. If the limit is reached, the least recently used entry is removed.",
            1000,
            ConfigType.INTEGER,
            "routingCache" ),

    PARAMETERIZE_DML(
            "runtime/parameterizeDML",
            "Whether DML queries should be parameterized.",
            true,
            ConfigType.BOOLEAN,
            "queryParameterizationGroup" ),

    PARAMETERIZE_INTERVALS(
            "runtime/parameterizeIntervals",
            "Whether intervals should be parameterized.",
            false,
            ConfigType.BOOLEAN,
            "queryParameterizationGroup" ),

    JOINED_TABLE_SCAN_CACHE(
            "runtime/joinedTableScanCache",
            "Whether to use the joined table scan caching.",
            false,
            ConfigType.BOOLEAN ),

    JOINED_TABLE_SCAN_CACHE_SIZE(
            "runtime/joinedTableScanCacheSize",
            "Size of the joined table scan cache. If the limit is reached, the least recently used entry is removed.",
            1000,
            ConfigType.INTEGER ),

    DATA_MIGRATOR_BATCH_SIZE(
            "runtime/dataMigratorBatchSize",
            "Batch size for data insertion on the target store.",
            1000,
            ConfigType.INTEGER ),

    UNIQUE_CONSTRAINT_ENFORCEMENT(
            "runtime/uniqueConstraintEnforcement",
            "Enable enforcement of uniqueness constraints.",
            false,
            ConfigType.BOOLEAN,
            "constraintEnforcementGroup" ),

    FOREIGN_KEY_ENFORCEMENT(
            "runtime/foreignKeyEnforcement",
            "Enable enforcement of foreign key constraints.",
            false,
            ConfigType.BOOLEAN,
            "constraintEnforcementGroup" ),

    POLYSTORE_INDEXES_ENABLED(
            "runtime/polystoreIndexesEnabled",
            "Enable and maintain indexes on the polystore level.",
            true,
            ConfigType.BOOLEAN,
            "polystoreIndexGroup" ),

    POLYSTORE_INDEXES_SIMPLIFY(
            "runtime/polystoreIndexesSimplify",
            "Enable query simplification using polystore level indexes.",
            false,
            ConfigType.BOOLEAN,
            "polystoreIndexGroup" ),

    DOCKER_INSTANCES(
            "runtime/dockerInstances",
            "Configure different docker instances, which can be used to place adapters on.",
            Collections.singletonList( new ConfigDocker( 0, "localhost", null, null, "localhost" )
                    .setDockerRunning( true ) ),
            ConfigType.INSTANCE_LIST,
            "dockerGroup" ),

    FILE_HANDLE_CACHE_SIZE(
            "runtime/fileHandleCacheSize",
            "Size (in Bytes) up to which media files are cached in-memory instead of creating a temporary file. Needs to be >= 0 and smaller than Integer.MAX_SIZE. Setting to zero disables caching of media files.",
            0,
            ConfigType.INTEGER,
            "processingExecutionGroup" ),

    MONITORING_CORE_POOL_SIZE(
            "runtime/corePoolSize",
            "The number of threads to keep in the pool for processing workload monitoring events, even if they are idle.",
            2,
            ConfigType.INTEGER,
            "monitoringSettingsQueueGroup" ),

    MONITORING_MAXIMUM_POOL_SIZE(
            "runtime/maximumPoolSize",
            "The maximum number of threads to allow in the pool used for processing workload monitoring events.",
            5000,
            ConfigType.INTEGER,
            "monitoringSettingsQueueGroup" ),

    MONITORING_POOL_KEEP_ALIVE_TIME(
            "runtime/keepAliveTime",
            "When the number of monitoring processing threads is greater than the core, this is the maximum time that excess idle threads will wait for new tasks before terminating.",
            10,
            ConfigType.INTEGER,
            "monitoringSettingsQueueGroup" ),

    TEMPERATURE_FREQUENCY_PROCESSING_INTERVAL(
            "runtime/partitionFrequencyProcessingInterval",
            "Time interval in seconds, how often the access frequency of all TEMPERATURE-partitioned tables is analyzed and redistributed",
            BackgroundTask.TaskSchedulingType.EVERY_MINUTE,
            ConfigType.ENUM,
            "temperaturePartitionProcessingSettingsGroup" ),

    CATALOG_DEBUG_MESSAGES(
            "runtime/catalogDebugMessages",
            "Enable output of catalog debug messages on the monitoring page.",
            false,
            ConfigType.BOOLEAN,
            "monitoringGroup" );


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
        //processingPage.withIcon( "fa fa-cogs" );
        final WebUiGroup planningGroup = new WebUiGroup( "planningGroup", processingPage.getId() );
        planningGroup.withTitle( "Query Planning" );
        final WebUiGroup parsingGroup = new WebUiGroup( "parsingGroup", processingPage.getId() );
        parsingGroup.withTitle( "Query Parsing" );
        final WebUiGroup implementationCachingGroup = new WebUiGroup( "implementationCachingGroup", processingPage.getId() );
        implementationCachingGroup.withTitle( "Implementation Caching" );
        final WebUiGroup queryParameterizationGroup = new WebUiGroup( "queryParameterizationGroup", processingPage.getId() );
        queryParameterizationGroup.withTitle( "Query Parameterization" );
        final WebUiGroup constraintEnforcementGroup = new WebUiGroup( "constraintEnforcementGroup", processingPage.getId() );
        constraintEnforcementGroup.withTitle( "Constraint Enforcement" );
        final WebUiGroup polystoreIndexGroup = new WebUiGroup( "polystoreIndexGroup", processingPage.getId() );
        polystoreIndexGroup.withTitle( "Polystore Indexes" );
        final WebUiGroup validationGroup = new WebUiGroup( "validationGroup", processingPage.getId() );
        validationGroup.withTitle( "Query Validation" );
        final WebUiGroup executionGroup = new WebUiGroup( "processingExecutionGroup", processingPage.getId() );
        executionGroup.withTitle( "Query Execution" );
        configManager.registerWebUiPage( processingPage );
        configManager.registerWebUiGroup( parsingGroup );
        configManager.registerWebUiGroup( planningGroup );
        configManager.registerWebUiGroup( implementationCachingGroup );
        configManager.registerWebUiGroup( queryParameterizationGroup );
        configManager.registerWebUiGroup( constraintEnforcementGroup );
        configManager.registerWebUiGroup( polystoreIndexGroup );
        configManager.registerWebUiGroup( validationGroup );
        configManager.registerWebUiGroup( executionGroup );

        // Routing
        final WebUiPage routingPage = new WebUiPage(
                "routing",
                "Query Routing",
                "Settings influencing the query routing behavior." );
        //routingPage.withIcon( "fa fa-map-signs" );
        final WebUiGroup routingCacheGroup = new WebUiGroup( "routingCache", routingPage.getId() );
        routingCacheGroup.withTitle( "Caching" );
        configManager.registerWebUiPage( routingPage );
        configManager.registerWebUiGroup( routingCacheGroup );

        // Statistics
        final WebUiPage queryStatisticsPage = new WebUiPage(
                "statisticsPage",
                "Statistics",
                "Settings on the stored data." );
        //queryStatisticsPage.withIcon( "fa fa-percent" );
        final WebUiGroup statisticSettingsGroup = new WebUiGroup( "statisticSettingsGroup", queryStatisticsPage.getId() );
        statisticSettingsGroup.withTitle( "Statistics Settings" );
        configManager.registerWebUiPage( queryStatisticsPage );
        configManager.registerWebUiGroup( statisticSettingsGroup );

        // Docker Settings
        final WebUiPage dockerPage = new WebUiPage(
                "dockerPage",
                "Docker",
                "Settings for the Docker-based data store deployment." );
        //dockerPage.withIcon( "fa fa-cube" );
        final WebUiGroup dockerGroup = new WebUiGroup( "dockerGroup", dockerPage.getId() );
        dockerGroup.withTitle( "Docker" );
        configManager.registerWebUiPage( dockerPage );
        configManager.registerWebUiGroup( dockerGroup );

        // UI specific setting
        final WebUiPage uiSettingsPage = new WebUiPage(
                "uiSettings",
                "Polypheny-UI",
                "Settings for this user interface." );
        //uiSettingsPage.withIcon( "fa fa-window-maximize" );
        configManager.registerWebUiPage( uiSettingsPage );
        final WebUiGroup uiSettingsDataViewGroup = new WebUiGroup( "uiSettingsDataViewGroup", uiSettingsPage.getId() );
        uiSettingsDataViewGroup.withTitle( "Data View" );
        configManager.registerWebUiGroup( uiSettingsDataViewGroup );
        final WebUiGroup monitoringGroup = new WebUiGroup( "monitoringGroup", uiSettingsPage.getId() );
        monitoringGroup.withTitle( "Monitoring" );
        configManager.registerWebUiGroup( monitoringGroup );

        // Workload Monitoring specific setting
        final WebUiPage monitoringSettingsPage = new WebUiPage(
                "monitoringSettings",
                "Workload Monitoring",
                "Settings for workload monitoring." );
        //monitoringSettingsPage.withIcon( "fa fa-line-chart" );
        final WebUiGroup monitoringSettingsQueueGroup = new WebUiGroup( "monitoringSettingsQueueGroup", monitoringSettingsPage.getId() );
        monitoringSettingsQueueGroup.withTitle( "Processing Queue" );
        configManager.registerWebUiPage( monitoringSettingsPage );
        configManager.registerWebUiGroup( monitoringSettingsQueueGroup );

        // Partitioning
        final WebUiPage partitionSettingsPage = new WebUiPage(
                "partitionSettings",
                "Partitioning",
                "Settings for partitioning" );
        //partitionSettingsPage.withIcon( "fa fa-thermometer-three-quarters" );
        final WebUiGroup temperaturePartitionProcessingSettingsGroup = new WebUiGroup( "temperaturePartitionProcessingSettingsGroup", partitionSettingsPage.getId() );
        temperaturePartitionProcessingSettingsGroup.withTitle( "TEMPERATURE Partition Processing" );
        configManager.registerWebUiPage( partitionSettingsPage );
        configManager.registerWebUiGroup( temperaturePartitionProcessingSettingsGroup );
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
                config = new ConfigEnum( key, description, defaultValue.getClass(), (Enum) defaultValue );
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

            case STRING_LIST:
                config = new ConfigList( key, (List<?>) defaultValue, String.class );
                break;

            case INSTANCE_LIST:
                config = new ConfigList( key, (List<?>) defaultValue, ConfigDocker.class );
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


    public List<String> getStringList() {
        return configManager.getConfig( key ).getStringList();
    }


    public <T> List<T> getList( Class<T> type ) {
        return configManager.getConfig( key ).getList( type );
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


    public void setEnum( Enum value ) {
        configManager.getConfig( key ).setEnum( value );
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


    public void setList( final List<ConfigScalar> values ) {
        configManager.getConfig( key ).setList( values );
    }


    public void setRequiresRestart( boolean requiresRestart ) {
        configManager.getConfig( key ).requiresRestart( requiresRestart );
    }


    public void addObserver( final ConfigListener listener ) {
        configManager.getConfig( key ).addObserver( listener );
    }


    public void removeObserver( final ConfigListener listener ) {
        configManager.getConfig( key ).removeObserver( listener );
    }


    public <T extends ConfigObject> T getWithId( Class<T> type, int id ) {
        Optional<T> optional = configManager.getConfig( key ).getList( type ).stream().filter( config -> config.id == id ).findAny();
        if ( optional.isPresent() ) {
            return optional.get();
        } else {
            throw new RuntimeException( "The was an error while retrieving the config." );
        }
    }


    public enum ConfigType {
        BOOLEAN, DECIMAL, DOUBLE, INTEGER, LONG, STRING, ENUM, BOOLEAN_TABLE, DECIMAL_TABLE, DOUBLE_TABLE, INTEGER_TABLE, LONG_TABLE, STRING_TABLE, BOOLEAN_ARRAY, DECIMAL_ARRAY, DOUBLE_ARRAY, INTEGER_ARRAY, LONG_ARRAY, STRING_ARRAY, STRING_LIST, INSTANCE_LIST
    }

}
