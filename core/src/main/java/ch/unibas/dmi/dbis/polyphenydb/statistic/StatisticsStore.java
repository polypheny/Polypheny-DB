package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.TransactionStat;
import ch.unibas.dmi.dbis.polyphenydb.TransactionStatType;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigBoolean;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigInteger;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigManager;
import ch.unibas.dmi.dbis.polyphenydb.config.WebUiGroup;
import ch.unibas.dmi.dbis.polyphenydb.config.WebUiPage;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGroup;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationTable;
import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTask;
import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTask.TaskPriority;
import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTask.TaskSchedulingType;
import ch.unibas.dmi.dbis.polyphenydb.util.background.BackgroundTaskManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * Stores all available statistics and updates INSERTs dynamically
 * DELETEs and UPADTEs should wait to be reprocessed
 */
@Slf4j
public class StatisticsStore<T extends Comparable<T>> implements Runnable {

    private static volatile StatisticsStore instance = null;

    @Getter
    public ConcurrentHashMap<String, HashMap<String, HashMap<String, StatisticColumn>>> statisticSchemaMap;

    private ConcurrentHashMap<String, PolySqlType> columnsToUpdate = new ConcurrentHashMap<>();
    private StatQueryProcessor sqlQueryInterface;

    private boolean storeOutOfSync = false;


    private StatisticsStore() {

        ConfigManager cm = ConfigManager.getInstance();
        cm.registerWebUiPage( new WebUiPage( "queryStatistics", "Dynamic Querying", "Statistics Settings which can assists with building a query with dynamic assistance." ) );
        cm.registerWebUiGroup( new WebUiGroup( "statisticSettings", "queryStatistics" ).withTitle( "Statistics Settings" ) );
        cm.registerConfig( new ConfigBoolean( "useStatistics", "Use statistics for query assistance.", true ).withUi( "statisticSettings" ) );
        cm.registerConfig( new ConfigInteger( "StatisticsPerColumn", "Number of rows per page in the data view", 10 ).withUi( "statisticSettings" ) );
        cm.registerConfig( new ConfigInteger( "maxCharUniqueVal", "Number of rows per page in the data view", 25 ).withUi( "statisticSettings" ) );

        this.statisticSchemaMap = new ConcurrentHashMap<>();
        displayInformation();

        // should only run when needed
        BackgroundTaskManager.INSTANCE.registerTask(
                this::sync,
                "Updated unsynced Statistic Columns.",
                TaskPriority.HIGH,
                TaskSchedulingType.EVERY_THIRTY_SECONDS );

        // security messure for now
        // BackgroundTaskManager.INSTANCE.registerTask( () -> System.out.println( "still running" ), "Check if store is still synced.", TaskPriority.LOW, TaskSchedulingType.EVERY_THIRTY_SECONDS );

    }


    public static synchronized StatisticsStore getInstance() {
        // To ensure only one instance is created
        if ( instance == null ) {
            instance = new StatisticsStore();
            instance.run();
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
        // TODO: switch back to {table = [columns], table = [columns]}
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
    synchronized private void reevaluateStore() {
        log.warn( "Resetting StatisticStore." );
        this.statisticSchemaMap.clear();
        this.columnsToUpdate.clear();

        // TODO: check why null
        if ( this.sqlQueryInterface == null ) {
            return;
        }

        for ( QueryColumn column : this.sqlQueryInterface.getAllColumns() ) {
            System.out.println( column.getFullName() );
            reevaluateColumn( column );

        }
        log.warn( "Finished resetting StatisticStore." );
    }


    /**
     * Method to sort a column into the different kinds of column types and hands it to the specific reevaluation
     */
    private void reevaluateColumn( QueryColumn column ) {
        if ( !this.sqlQueryInterface.hasData( column.getFullTableName(), column.getFullName() ) ) {
            return;
        }
        if ( column.getType().isNumericalType() ) {
            this.reevaluateNumericalColumn( column );
        } else if ( column.getType().isCharType() ) {
            this.reevaluateAlphabeticalColumn( column );
        }
    }


    private void reevaluateNumericalColumn( QueryColumn column ) {
        StatQueryColumn min = this.getAggregateColumn( column, "MIN" );
        StatQueryColumn max = this.getAggregateColumn( column, "MAX" );
        StatQueryColumn unique = this.getUniqueValues( column );
        Integer count = this.getCount( column );
        NumericalStatisticColumn<String> statisticColumn = new NumericalStatisticColumn<>( QueryColumn.getSplitColumn( column.getFullName() ), column.getType() );
        statisticColumn.setMin( min.getData()[0] );
        statisticColumn.setMax( max.getData()[0] );
        statisticColumn.setUniqueValues( Arrays.asList( unique.getData() ) );
        statisticColumn.setCount( count );

        put( column.getSchema(), column.getTable(), column.getName(), statisticColumn );
    }


    private void reevaluateAlphabeticalColumn( QueryColumn column ) {
        StatQueryColumn unique = this.getUniqueValues( column );
        Integer count = this.getCount( column );

        AlphabeticStatisticColumn<String> statisticColumn = new AlphabeticStatisticColumn<>( QueryColumn.getSplitColumn( column.getFullName() ), column.getType() );
        statisticColumn.setUniqueValues( Arrays.asList( unique.getData() ) );
        statisticColumn.setCount( count );

        put( column.getSchema(), column.getTable(), column.getName(), statisticColumn );
    }


    private void put( String schema, String table, String column, StatisticColumn statisticColumn ) {
        if ( !this.statisticSchemaMap.containsKey( schema ) ) {
            this.statisticSchemaMap.put( schema, new HashMap<>() );
        }

        if ( !this.statisticSchemaMap.get( schema ).containsKey( table ) ) {
            this.statisticSchemaMap.get( schema ).put( table, new HashMap<>() );
        }

        if ( !this.statisticSchemaMap.get( schema ).get( table ).containsKey( column ) ) {
            this.statisticSchemaMap.get( schema ).get( table ).put( column, statisticColumn );
        }

    }


    /**
     * Method to get a generic Aggregate Stat
     *
     * @return a StatQueryColumn which contains the requested value
     */
    private StatQueryColumn getAggregateColumn( QueryColumn column, String aggregate ) {
        return getAggregateColumn( column.getFullName(), column.getFullTableName(), aggregate );
    }


    private StatQueryColumn getAggregateColumn( String column, String table, String aggregate ) {
        String query = "SELECT " + aggregate + " (" + column + ") FROM " + table + getStatQueryLimit();
        return this.sqlQueryInterface.selectOneStat( query );
    }


    private StatQueryColumn getUniqueValues( QueryColumn column ) {
        //TODO ASK needs limit, else throws error when casting to autoclose
        String query = "SELECT " + column.getFullName() + " FROM " + column.getFullTableName() + " GROUP BY " + column.getFullName() + getStatQueryLimit();
        return this.sqlQueryInterface.selectOneStat( query );
    }


    private Integer getCount( QueryColumn column ) {
        String query = "SELECT COUNT(" + column.getFullName() + ") FROM " + column.getFullTableName();
        return Integer.parseInt( this.sqlQueryInterface.selectOneStat( query ).getData()[0] );
    }


    private String getStatQueryLimit() {
        return " LIMIT " + ConfigManager.getInstance().getConfig( "StatisticsPerColumn" ).getInt();
    }


    public void setSqlQueryInterface( StatQueryProcessor statQueryProcessor ) {
        this.sqlQueryInterface = statQueryProcessor;

        this.reevaluateStore();

    }


    public void displayInformation() {
        InformationManager im = InformationManager.getInstance();

        // TODO: only testwise replace with actual changing data later
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
                            String values = ((AlphabeticStatisticColumn) v).getUniqueValues().toString();
                            alphabeticalInformation.addRow(
                                    name,
                                    values.substring( 0, Math.min( values.length(), ConfigManager.getInstance().getConfig( "maxCharUniqueVal" ).getInt() ) )
                            );
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
    public void apply( ArrayList<TransactionStat> stats ) {

        stats.forEach( s -> {
            TransactionStatType type = s.getTransactionType();
            // TODO: better prefiltering
            if ( type == TransactionStatType.INSERT ) {
                insert( s.getSchema(), s.getTableName(), s.getColumnName(), (T) s.getData() );
            } else if ( type == TransactionStatType.DELETE || type == TransactionStatType.UPDATE ) {
                // TODO: wait to be evalutated

                if ( statisticSchemaMap.containsKey( s.getColumnName() ) ) {
                    getColumn( s.getSchema(), s.getTable(), s.getColumn() ).setUpdated( false );
                    columnsToUpdate.put( s.getColumnName(), getColumn( s.getSchema(), s.getTable(), s.getColumn() ).getType() );
                } else {
                    columnsToUpdate.put( s.getColumnName(), sqlQueryInterface.getColumnType( s.getColumnName() ) );
                }
            }
        } );
    }


    /**
     * Checks if store is in sync or reevalutates
     * else Method goes through all columns for update
     */
    public synchronized void sync() {
        // TODO: real disable of query assistance
        if ( !ConfigManager.getInstance().getConfig( "useStatistics" ).getBoolean() ) {
            return;
        }

        reevaluateStore();
        /*
        if(storeOutOfSync){

            storeOutOfSync = false;
        }else {
            columnsToUpdate.forEach( ( column, type ) -> {
                columns.remove( column );
                reevaluateColumn( new QueryColumn( column, type ) );
            } );
        }*/

        columnsToUpdate.clear();
    }


    private void checkSync() {
    }


    @Override
    public void run() {
    }
}
