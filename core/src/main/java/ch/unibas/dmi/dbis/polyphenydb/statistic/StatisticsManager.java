package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.config.Config;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigBoolean;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigEnum;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigInteger;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigManager;
import ch.unibas.dmi.dbis.polyphenydb.config.WebUiGroup;
import ch.unibas.dmi.dbis.polyphenydb.config.WebUiPage;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGroup;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationTable;
import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTask.TaskPriority;
import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTask.TaskSchedulingType;
import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTaskManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * Stores all available statistics and updates INSERTs dynamically
 * DELETEs and UPADTEs should wait to be reprocessed
 */
@Slf4j
public class StatisticsManager<T extends Comparable<T>> {

    private static volatile StatisticsManager instance = null;

    @Getter
    public volatile ConcurrentHashMap<String, HashMap<String, HashMap<String, StatisticColumn>>> statisticSchemaMap;

    private StatisticQueryProcessor sqlQueryInterface;

    private ExecutorService threadPool = Executors.newSingleThreadExecutor();

    @Setter
    @Getter
    private String revalId = null;


    private StatisticsManager() {

        ConfigManager cm = ConfigManager.getInstance();
        cm.registerWebUiPage( new WebUiPage( "queryStatistics", "Dynamic Querying", "Statistics Settings which can assists with building a query with dynamic assistance." ) );
        cm.registerWebUiGroup( new WebUiGroup( "statisticSettings", "queryStatistics" ).withTitle( "Statistics Settings" ) );
        cm.registerConfig( new ConfigBoolean( "useDynamicQuerying", "Use statistics for query assistance.", true ).withUi( "statisticSettings", 0 ) );
        cm.registerConfig( new ConfigBoolean( "activeTracking", "All transactions are tracked and statistics collected.", true ).withUi( "statisticSettings", 1 ) );
        cm.registerConfig( new ConfigBoolean( "passiveTracking", "Reevaluated the whole store, after a set time.", false ).withUi( "statisticSettings", 2 ) );
        cm.registerConfig( new ConfigInteger( "StatisticColumnBuffer", "Number of rows per page in the data view", 5 ).withUi( "statisticSettings", 10 ) );
        cm.registerConfig( new ConfigInteger( "maxCharUniqueVal", "Number of rows per page in the data view", 10 ).withUi( "statisticSettings", 11 ) );

        cm.registerConfig( new ConfigEnum( "passiveTrackingRate", TaskSchedulingType.class, TaskSchedulingType.EVERY_THIRTY_SECONDS ) );

        this.statisticSchemaMap = new ConcurrentHashMap<>();
        displayInformation();
        configureBackgroundTask();


    }


    /**
     * Registers toggleable option to active passive querying
     */
    private void configureBackgroundTask() {
        ConfigManager.getInstance().getConfig( "passiveTracking" ).addObserver( new Config.ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                registerToggable( c );
            }


            @Override
            public void restart( Config c ) {
                registerToggable( c );
            }


            public void registerToggable( Config c ) {
                String id = StatisticsManager.getInstance().getRevalId();
                if ( c.getBoolean() && id == null ) {
                    String revalId = BackgroundTaskManager.INSTANCE.registerTask(
                            StatisticsManager.this::asyncReevaluateStore,
                            "Reevalute StatisticsManager.",
                            TaskPriority.LOW,
                            (TaskSchedulingType) ConfigManager.getInstance().getConfig( "backgroundTaskRate" ).getEnum() );
                    StatisticsManager.getInstance().setRevalId( revalId );
                } else if ( ! c.getBoolean() && id != null ) {
                    BackgroundTaskManager.INSTANCE.removeBackgroundTask( StatisticsManager.getInstance().getRevalId() );
                    StatisticsManager.getInstance().setRevalId( null );
                }
            }
        } );
    }


    public static StatisticsManager getInstance() {
        // To ensure only one instance is created
        synchronized ( StatisticsManager.class ) {
            if ( instance == null ) {
                instance = new StatisticsManager();
            }
        }
        return instance;
    }


    /**
     * Gets the specific statisticColumn if it exists in the tracked columns
     * else null
     *
     * @return the statisticColumn which matches the criteria
     */
    private StatisticColumn<T> getColumn( String schema, String table, String column ) {
        if ( this.statisticSchemaMap.containsKey( schema ) ) {
            if ( this.statisticSchemaMap.get( schema ).containsKey( table ) ) {
                if ( this.statisticSchemaMap.get( schema ).get( table ).containsKey( column ) ) {
                    return this.statisticSchemaMap.get( schema ).get( table ).get( column );
                }
            }
        }
        return null;
    }


    /**
     * Adds a new column to the tracked columns and sorts it correctly
     *
     * @param qualifiedColumn column name
     * @param type the type of the new column
     */
    private void addColumn( String qualifiedColumn, PolySqlType type ) {
        String[] splits = QueryColumn.getSplitColumn( qualifiedColumn );
        if ( type.isNumericalType() ) {
            put( splits[0], splits[1], splits[2], new NumericalStatisticColumn<>( splits, type ) );
        } else if ( type.isCharType() ) {
            put( splits[0], splits[1], splits[2], new AlphabeticStatisticColumn<>( splits, type ) );
        }
    }


    /**
     * Reset all statistics and reevaluate them
     */
    private void reevaluateStore() {
        if ( this.sqlQueryInterface == null ) {
            return;
        }
        log.debug( "Resetting StatisticStore." );
        ConcurrentHashMap statisticSchemaMapCopy = new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticColumn>>>();

        for ( QueryColumn column : this.sqlQueryInterface.getAllColumns() ) {
            StatisticColumn col = reevaluateColumn( column );
            if ( col != null ) {
                put( statisticSchemaMapCopy, column.getSchema(), column.getTable(), column.getName(), col );
            }

        }
        replaceStatistics( statisticSchemaMapCopy );
        log.debug( "Finished resetting StatisticStore." );
    }


    /**
     * Gets a columns of a table and reevaluates them
     *
     * @param qualifiedTable table name
     */
    private void reevaluateTable( String qualifiedTable ) {
        if ( this.sqlQueryInterface == null ) {
            return;
        }

        String[] splits = qualifiedTable.replace( "\"", "" ).split( "\\." );
        if ( splits.length != 2 ) {
            return;
        }

        List<QueryColumn> res = this.sqlQueryInterface.getAllColumns( splits[0], splits[1] );

        for ( QueryColumn column : res ) {
            StatisticColumn col = reevaluateColumn( column );
            if ( col != null ) {
                put( column.getSchema(), column.getTable(), column.getName(), col );
            }

        }

    }


    /**
     * replace the the tracked statistics with other statistics
     */
    private synchronized void replaceStatistics( ConcurrentHashMap map ) {
        this.statisticSchemaMap = new ConcurrentHashMap<>( map );
    }


    /**
     * Method to sort a column into the different kinds of column types and hands it to the specific reevaluation
     */
    private StatisticColumn reevaluateColumn( QueryColumn column ) {
        if ( ! this.sqlQueryInterface.hasData( column.getSchema(), column.getTable(), column.getName() ) ) {
            return null;
        }
        if ( column.getType().isNumericalType() ) {
            return this.reevaluateNumericalColumn( column );
        } else if ( column.getType().isCharType() ) {
            return this.reevaluateAlphabeticalColumn( column );
        }
        return null;
    }


    /**
     * Reevaluates a numerical column, with the configured statistics
     */
    private StatisticColumn reevaluateNumericalColumn( QueryColumn column ) {
        StatisticQueryColumn min = this.getAggregateColumn( column, "MIN" );
        StatisticQueryColumn max = this.getAggregateColumn( column, "MAX" );
        StatisticQueryColumn unique = this.getUniqueValues( column );
        Integer count = this.getCount( column );
        NumericalStatisticColumn<String> statisticColumn = new NumericalStatisticColumn<>( QueryColumn.getSplitColumn( column.getQualifiedColumnName() ), column.getType() );
        if ( min != null ) {
            statisticColumn.setMin( min.getData()[0] );
        }
        if ( max != null ) {
            statisticColumn.setMax( max.getData()[0] );
        }

        assignUnique( unique, statisticColumn );

        statisticColumn.setCount( count );

        return statisticColumn;
    }


    /**
     * Helper method tho assign unique values or set isFull if too much exist
     *
     * @param unique the unique values
     * @param statisticColumn the column in which the values should be inserted
     */
    private void assignUnique( StatisticQueryColumn unique, StatisticColumn<String> statisticColumn ) {
        int maxLength = ConfigManager.getInstance().getConfig( "StatisticColumnBuffer" ).getInt();
        if ( unique == null || unique.getData() == null ) {
            return;
        }
        if ( unique.getData().length <= maxLength ) {
            statisticColumn.setUniqueValues( Arrays.asList( unique.getData() ) );
        } else {
            statisticColumn.setFull( true );
        }
    }


    /**
     * Reevaluates an alphabetical column, with the configured statistics
     */
    private StatisticColumn reevaluateAlphabeticalColumn( QueryColumn column ) {
        StatisticQueryColumn unique = this.getUniqueValues( column );
        Integer count = this.getCount( column );

        AlphabeticStatisticColumn<String> statisticColumn = new AlphabeticStatisticColumn<>( QueryColumn.getSplitColumn( column.getQualifiedColumnName() ), column.getType() );
        assignUnique( unique, statisticColumn );
        statisticColumn.setCount( count );

        return statisticColumn;
    }


    /**
     * Places a column at the correct position in the schemaMap
     *
     * @param map which schemaMap should be used
     * @param statisticColumn the Column with its statistics
     */
    private void put( ConcurrentHashMap<String, HashMap<String, HashMap<String, StatisticColumn>>> map, String schema, String table, String column, StatisticColumn statisticColumn ) {
        if ( ! map.containsKey( schema ) ) {
            map.put( schema, new HashMap<>() );
        }

        if ( ! map.get( schema ).containsKey( table ) ) {
            map.get( schema ).put( table, new HashMap<>() );
        }
        map.get( schema ).get( table ).put( column, statisticColumn );

    }


    private void put( String schema, String table, String column, StatisticColumn statisticColumn ) {
        put( this.statisticSchemaMap, schema, table, column, statisticColumn );

    }


    /**
     * Method to get a generic Aggregate Stat
     * null if no result is available
     *
     * @return a StatQueryColumn which contains the requested value
     */
    private StatisticQueryColumn getAggregateColumn( QueryColumn column, String aggregate ) {
        return getAggregateColumn( column.getSchema(), column.getTable(), column.getName(), aggregate );
    }


    /**
     * Queries the database with a aggreagate query
     *
     * @param aggregate the aggregate funciton to us
     */
    private StatisticQueryColumn getAggregateColumn( String schema, String table, String column, String aggregate ) {
        String query = "SELECT " + aggregate + " (" + StatisticQueryProcessor.buildName( schema, table, column ) + ") FROM " + StatisticQueryProcessor.buildName( schema, table ) + getStatQueryLimit();
        return this.sqlQueryInterface.selectOneStat( query );
    }


    /**
     * Gets the configured amount + 1 of unique values per column
     *
     * @return the unique values
     */
    private StatisticQueryColumn getUniqueValues( QueryColumn column ) {
        String tableName = StatisticQueryProcessor.buildName( column.getSchema(), column.getTable() );
        String columnName = StatisticQueryProcessor.buildName( column.getSchema(), column.getTable(), column.getName() );
        String query = "SELECT " + columnName + " FROM " + tableName + " GROUP BY " + columnName + getStatQueryLimit( 1 );
        return this.sqlQueryInterface.selectOneStat( query );
    }


    /**
     * Gets the amount of entries for a column
     */
    private Integer getCount( QueryColumn column ) {
        String tableName = StatisticQueryProcessor.buildName( column.getSchema(), column.getTable() );
        String columnName = StatisticQueryProcessor.buildName( column.getSchema(), column.getTable(), column.getName() );
        String query = "SELECT COUNT(" + columnName + ") FROM " + tableName;
        StatisticQueryColumn res = this.sqlQueryInterface.selectOneStat( query );

        if ( res != null && res.getData() != null && res.getData().length != 0 ) {
            try {
                return Integer.parseInt( res.getData()[0] );
            } catch ( NumberFormatException e ) {
                log.error( "Count could not be parsed for column {}.", column.getQualifiedColumnName(), e );
            }
        }
        return 0;
    }


    private String getStatQueryLimit() {
        return getStatQueryLimit( 0 );
    }


    private String getStatQueryLimit( int add ) {
        return " LIMIT " + (ConfigManager.getInstance().getConfig( "StatisticColumnBuffer" ).getInt() + add);
    }


    public void setSqlQueryInterface( StatisticQueryProcessor statisticQueryProcessor ) {
        this.sqlQueryInterface = statisticQueryProcessor;

        this.asyncReevaluateStore();

    }


    /**
     * Configures and registers the statistics informationpage for the frontend
     */
    public void displayInformation() {
        InformationManager im = InformationManager.getInstance();

        InformationPage page = new InformationPage( "statistics", "Statistics" );
        im.addPage( page );

        InformationGroup contentGroup = new InformationGroup( page, "Column Statistic Status" );
        im.addGroup( contentGroup );

        InformationTable statisticsInformation = new InformationTable( contentGroup, Arrays.asList( "Column Name", "Type", "Count", "Updated" ) );

        im.registerInformation( statisticsInformation );

        BackgroundTaskManager.INSTANCE.registerTask(
                () -> {
                    statisticsInformation.reset();
                    statisticSchemaMap.values().forEach( schema -> schema.values().forEach( table -> table.forEach( ( k, v ) -> {
                        String name = v.getSchema() + "." + v.getTable() + "." + v.getColumn();
                        if ( v.isUpdated() ) {
                            statisticsInformation.addRow( name, v.getType().name(), v.getCount(), "✔" );
                        } else {
                            statisticsInformation.addRow( name, v.getType().name(), v.getCount(), "❌" );
                        }

                    } ) ) );
                },
                "Reset the Statistic InformationPage for the dynamicQuering",
                TaskPriority.LOW,
                TaskSchedulingType.EVERY_FIVE_SECONDS
        );

        InformationGroup alphabeticalGroup = new InformationGroup( page, "Alphabetical Statistics" );
        im.addGroup( alphabeticalGroup );

        InformationGroup numericalGroup = new InformationGroup( page, "Numerical Statistics" );
        im.addGroup( numericalGroup );

        InformationTable numericalInformation = new InformationTable( numericalGroup, Arrays.asList( "Column Name", "Min", "Max" ) );

        InformationTable alphabeticalInformation = new InformationTable( alphabeticalGroup, Arrays.asList( "Column Name", "Unique Values" ) );

        im.registerInformation( numericalInformation );
        im.registerInformation( alphabeticalInformation );

        BackgroundTaskManager.INSTANCE.registerTask(
                () -> {
                    numericalInformation.reset();
                    alphabeticalInformation.reset();
                    statisticSchemaMap.values().forEach( schema -> schema.values().forEach( table -> table.forEach( ( k, v ) -> {
                        String name = v.getSchema() + "." + v.getTable() + "." + v.getColumn();
                        if ( v instanceof NumericalStatisticColumn ) {

                            if ( ((NumericalStatisticColumn) v).getMin() != null && ((NumericalStatisticColumn) v).getMax() != null ) {
                                numericalInformation.addRow( name, ((NumericalStatisticColumn) v).getMin().toString(), ((NumericalStatisticColumn) v).getMax().toString() );
                            } else {
                                numericalInformation.addRow( name, "❌", "❌" );
                            }

                        } else {
                            String values = v.getUniqueValues().toString();
                            if ( ! v.isFull ) {
                                alphabeticalInformation.addRow( name, values );
                            } else {
                                alphabeticalInformation.addRow( name, "is Full" );
                            }

                        }

                    } ) ) );

                }, "Reset Min Max for all numericalColumns.",
                TaskPriority.LOW,
                TaskSchedulingType.EVERY_FIVE_SECONDS );

    }


    public void asyncReevaluateStore() {
        threadPool.execute( this::reevaluateStore );
    }


    /**
     * Reevalutes all tables which received changes impacting their statistic data
     *
     * @param changedTables all tables which got changed in a transaction
     */
    public void apply( List<String> changedTables ) {
        threadPool.execute( () -> {
            changedTables.forEach( this::reevaluateTable );
        } );
    }
}
