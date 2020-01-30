package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigBoolean;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * Stores all available statistics and updates INSERTs dynamically
 * DELETEs and UPADTEs should wait to be reprocessed
 */
@Slf4j
public class StatisticsStore<T extends Comparable<T>> {

    private static volatile StatisticsStore instance = null;

    @Getter
    public volatile ConcurrentHashMap<String, HashMap<String, HashMap<String, StatisticColumn>>> statisticSchemaMap;

    private ConcurrentHashMap<String, PolySqlType> columnsToUpdate = new ConcurrentHashMap<>();
    private StatQueryProcessor sqlQueryInterface;

    @Setter
    @Getter
    private String revalId = null;


    private StatisticsStore() {

        ConfigManager cm = ConfigManager.getInstance();
        cm.registerWebUiPage( new WebUiPage( "queryStatistics", "Dynamic Querying", "Statistics Settings which can assists with building a query with dynamic assistance." ) );
        cm.registerWebUiGroup( new WebUiGroup( "statisticSettings", "queryStatistics" ).withTitle( "Statistics Settings" ) );
        cm.registerConfig( new ConfigBoolean( "useDynamicQuerying", "Use statistics for query assistance.", true ).withUi( "statisticSettings" ) );
        cm.registerConfig( new ConfigInteger( "StatisticColumnBuffer", "Number of rows per page in the data view", 5 ).withUi( "statisticSettings" ) );
        cm.registerConfig( new ConfigInteger( "maxCharUniqueVal", "Number of rows per page in the data view", 10 ).withUi( "statisticSettings" ) );

        this.statisticSchemaMap = new ConcurrentHashMap<>();
        displayInformation();

        // When low WORKLOAD flags works this comment can be removed
        /*BackgroundTaskManager.INSTANCE.registerTask(
                StatisticsStore.this::reevaluateStore,
                "Reevalate Statistic Store.",
                TaskPriority.LOW,
                TaskSchedulingType.Workload );

        ConfigManager.getInstance().getConfig( "useDynamicQuerying" ).addObserver( new ConfigListener() {
            @Override
            public void onConfigChange( Config c ) {
                String id = StatisticsStore.getInstance().getRevalId();
                if ( c.getBoolean() && id == null ) {
                    String revalId = BackgroundTaskManager.INSTANCE.registerTask(
                            StatisticsStore.this::reevaluateStore,
                            "Reevalute store.",
                            TaskPriority.LOW,
                            TaskSchedulingType.WORKLOAD );
                    StatisticsStore.getInstance().setRevalId( revalId );
                } else if ( !c.getBoolean() && id != null ) {
                    BackgroundTaskManager.INSTANCE.removeBackgroundTask( StatisticsStore.getInstance().getRevalId() );
                    StatisticsStore.getInstance().setRevalId( null );
                }

            }


            @Override
            public void restart( Config c ) {

            }
        } );*/
    }


    public static synchronized StatisticsStore getInstance() {
        // To ensure only one instance is created
        if ( instance == null ) {
            instance = new StatisticsStore();
        }
        return instance;
    }


    private void insert( String schemaTableColumn, T val ) {
        String[] splits = QueryColumn.getSplitColumn( schemaTableColumn );
        insert( splits[0], splits[1], splits[2], val );
    }


    /**
     * adds data for a column to the store
     *
     * @param schema schema name
     * @param table table name in form [schema].[table]
     * @param column column name in form [schema].[table].[column]
     */
    private void insert( String schema, String table, String column, T val ) {
        PolySqlType type = this.sqlQueryInterface.getColumnType( column );
        insert( schema, table, column, type, val );
    }


    private void insert( String schema, String table, String column, PolySqlType type, T val ) {
        if ( !this.statisticSchemaMap.containsKey( column ) ) {
            addColumn( column, type );
        }
        getColumn( schema, table, column ).insert( val );
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
     * @param column column name as [schema].[table].[column]
     * @param type the type of the new column
     */
    private void addColumn( String column, PolySqlType type ) {
        String[] splits = QueryColumn.getSplitColumn( column );
        if ( type.isNumericalType() ) {
            put( splits[0], splits[1], splits[2], new NumericalStatisticColumn<>( splits, type ) );
        } else if ( type.isCharType() ) {
            put( splits[0], splits[1], splits[2], new AlphabeticStatisticColumn<>( splits, type ) );
        }
    }


    public void addAll( String schema, String table, String column, PolySqlType type, List<T> vals ) {
        vals.forEach( val -> {
            insert( schema, table, column, type, val );
        } );
    }


    /**
     * Reset all statistics and reevaluate them
     */
    private void reevaluateStore() {
        if ( this.sqlQueryInterface == null ) {
            return;
        }
        log.warn( "Resetting StatisticStore." );
        this.columnsToUpdate.clear();
        ConcurrentHashMap statisticSchemaMapCopy = new ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, StatisticColumn>>>();

        for ( QueryColumn column : this.sqlQueryInterface.getAllColumns() ) {
            StatisticColumn col = reevaluateColumn( column );
            if ( col != null ) {
                put( statisticSchemaMapCopy, column.getSchema(), column.getTable(), column.getName(), col );
            }

        }
        replaceStatistics( statisticSchemaMapCopy );
        log.warn( "Finished resetting StatisticStore." );
    }


    /**
     * Gets a columns of a table and reevaluates them
     *
     * @param table the table name as  [schema].[table]
     */
    private void reevaluateTable( String table ) {
        if ( this.sqlQueryInterface == null ) {
            return;
        }

        String[] splits = table.replace( "\"", "" ).split( "\\." );
        if ( splits.length != 2 ) {
            return;
        }

        ArrayList<QueryColumn> res = this.sqlQueryInterface.getAllColumns( splits[0], splits[1] );

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
        if ( !this.sqlQueryInterface.hasData( column.getSchema(), column.getTable(), column.getName() ) ) {
            return null;
        }
        if ( column.getType().isNumericalType() ) {
            return this.reevaluateNumericalColumn( column );
        } else if ( column.getType().isCharType() ) {
            return this.reevaluateAlphabeticalColumn( column );
        }
        return null;
    }


    private StatisticColumn reevaluateNumericalColumn( QueryColumn column ) {
        StatQueryColumn min = this.getAggregateColumn( column, "MIN" );
        StatQueryColumn max = this.getAggregateColumn( column, "MAX" );
        StatQueryColumn unique = this.getUniqueValues( column );
        Integer count = this.getCount( column );
        NumericalStatisticColumn<String> statisticColumn = new NumericalStatisticColumn<>( QueryColumn.getSplitColumn( column.getFullName() ), column.getType() );
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
    private void assignUnique( StatQueryColumn unique, StatisticColumn<String> statisticColumn ) {
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


    private StatisticColumn reevaluateAlphabeticalColumn( QueryColumn column ) {
        StatQueryColumn unique = this.getUniqueValues( column );
        Integer count = this.getCount( column );

        AlphabeticStatisticColumn<String> statisticColumn = new AlphabeticStatisticColumn<>( QueryColumn.getSplitColumn( column.getFullName() ), column.getType() );
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
        if ( !map.containsKey( schema ) ) {
            map.put( schema, new HashMap<>() );
        }

        if ( !map.get( schema ).containsKey( table ) ) {
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
    private StatQueryColumn getAggregateColumn( QueryColumn column, String aggregate ) {
        return getAggregateColumn( column.getSchema(), column.getTable(), column.getName(), aggregate );
    }


    /**
     * Queries the database with a aggreagate query
     *
     * @param aggregate the aggregate funciton to us
     */
    private StatQueryColumn getAggregateColumn( String schema, String table, String column, String aggregate ) {
        String query = "SELECT " + aggregate + " (" + StatQueryProcessor.buildName( schema, table, column ) + ") FROM " + StatQueryProcessor.buildName( schema, table ) + getStatQueryLimit();
        return this.sqlQueryInterface.selectOneStat( query );
    }


    /**
     * Gets the configured amount + 1 of unique values per column
     *
     * @return the unique values
     */
    private StatQueryColumn getUniqueValues( QueryColumn column ) {
        String tableName = StatQueryProcessor.buildName( column.getSchema(), column.getTable() );
        String columnName = StatQueryProcessor.buildName( column.getSchema(), column.getTable(), column.getName() );
        String query = "SELECT " + columnName + " FROM " + tableName + " GROUP BY " + columnName + getStatQueryLimit( 1 );
        return this.sqlQueryInterface.selectOneStat( query );
    }


    /**
     * Gets the amount of entries for a column
     */
    private Integer getCount( QueryColumn column ) {
        String tableName = StatQueryProcessor.buildName( column.getSchema(), column.getTable() );
        String columnName = StatQueryProcessor.buildName( column.getSchema(), column.getTable(), column.getName() );
        String query = "SELECT COUNT(" + columnName + ") FROM " + tableName;
        StatQueryColumn res = this.sqlQueryInterface.selectOneStat( query );

        if ( res != null && res.getData() != null && res.getData().length != 0 ) {
            try {
                return Integer.parseInt( res.getData()[0] );
            } catch ( NumberFormatException e ) {
                log.error( "Count could not be parsed." );
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


    public void setSqlQueryInterface( StatQueryProcessor statQueryProcessor ) {
        this.sqlQueryInterface = statQueryProcessor;

        this.reevaluateStore();

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
                            // TODO: concationate to long entries
                            String values = ((AlphabeticStatisticColumn) v).getUniqueValues().toString();
                            if ( !v.isFull ) {
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


    /**
     * Transaction hands it changes so they can be added.
     *
     * @param stats all changes for this store
     */
    public void apply( ArrayList<String> stats ) {
        stats.forEach( this::reevaluateTable );

    }


    /**
     * Checks if store is in sync or reevalutates
     * else Method goes through all columns for update
     */
    public synchronized void sync() {
        if ( !ConfigManager.getInstance().getConfig( "useDynamicQuerying" ).getBoolean() ) {
            return;
        }

        reevaluateStore();
    }
}
