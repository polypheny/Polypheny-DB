package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.TransactionManager;
import ch.unibas.dmi.dbis.polyphenydb.TransactionStat;
import ch.unibas.dmi.dbis.polyphenydb.TransactionStatType;
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

    private static StatisticsStore instance = null;

    @Getter
    public ConcurrentHashMap<String, StatisticColumn> columns;

    private ConcurrentHashMap<String, PolySqlType> columnsToUpdate = new ConcurrentHashMap<>();
    private StatQueryProcessor sqlQueryInterface;

    private boolean storeOutOfSync = false;


    private StatisticsStore() {
        this.columns = new ConcurrentHashMap<>();
        displayInformation();

        ConfigManager cm = ConfigManager.getInstance();
        cm.registerConfig( new ConfigInteger( "StatisticsPerColumn", "Number of rows per page in the data view", 10 ).withUi( "statisticView" ) );

        BackgroundTaskManager.INSTANCE.registerTask( new StatisticsStoreWorker(), "Updated unsynced Statistic Columns.", TaskPriority.LOW, TaskSchedulingType.EVERY_TEN_SECONDS );

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
        // TODO: switch back to {table = [columns], table = [columns]}
        if ( !this.columns.containsKey( column ) ) {
            addColumn( column, type );
        }
        columns.get( column ).insert( val );
    }


    private void addColumn( String column, PolySqlType type ) {
        if ( type.isNumericalType() ) {
            this.columns.put( column, new NumericalStatisticColumn<>( QueryColumn.getSplitColumn( column ), type ) );
        } else if ( type.isCharType() ) {
            this.columns.put( column, new AlphabeticStatisticColumn<>( QueryColumn.getSplitColumn( column ), type ) );
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
    public void reevaluateStore() {
        this.columns.clear();

        for ( QueryColumn column : this.sqlQueryInterface.getAllColumns() ) {
            System.out.println( column.getFullName() );
            reevaluateColumn( column );

        }

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

        this.columns.put( column.getFullName(), statisticColumn );
    }


    private void reevaluateAlphabeticalColumn( QueryColumn column ) {
        StatQueryColumn unique = this.getUniqueValues( column );
        Integer count = this.getCount( column );

        AlphabeticStatisticColumn<String> statisticColumn = new AlphabeticStatisticColumn<>( QueryColumn.getSplitColumn( column.getFullName() ), column.getType() );
        statisticColumn.setUniqueValues( Arrays.asList( unique.getData() ) );
        statisticColumn.setCount( count );

        this.columns.put( column.getFullName(), statisticColumn );
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

        columns.forEach( ( k, v ) -> {
            statisticsInformation.addRow( k, v.getType().toString(), v.getCount(), Boolean.toString( v.isUpdated() ) );
        } );

        im.registerInformation( statisticsInformation );

        Timer t2 = new Timer();
        t2.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                statisticsInformation.reset();
                columns.forEach( ( k, v ) -> {
                    if ( v.isUpdated() ) {
                        statisticsInformation.addRow( k, v.getType().name(), v.getCount(), "✔" );
                    } else {
                        statisticsInformation.addRow( k, v.getType().name(), v.getCount(), "❌" );
                    }

                } );
            }
        }, 5000, 5000 );

        InformationGroup alphabeticalGroup = new InformationGroup( page, "Alphabetical Statistics" );
        im.addGroup( alphabeticalGroup );

        InformationGroup numericalGroup = new InformationGroup( page, "Numerical Statistics" );
        im.addGroup( numericalGroup );

        InformationTable numericalInformation = new InformationTable( numericalGroup, Arrays.asList( "Column Name", "Min", "Max" ) );

        InformationTable alphabeticalInformation = new InformationTable( alphabeticalGroup, Arrays.asList( "Column Name", "Unique Values" ) );

        im.registerInformation( numericalInformation );
        im.registerInformation( alphabeticalInformation );

        Timer t3 = new Timer();
        t3.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                numericalInformation.reset();
                alphabeticalInformation.reset();
                columns.forEach( ( k, v ) -> {
                    if ( v instanceof NumericalStatisticColumn ) {
                        if ( ((NumericalStatisticColumn) v).getMin() != null && ((NumericalStatisticColumn) v).getMax() != null ) {
                            numericalInformation.addRow( k, ((NumericalStatisticColumn) v).getMin().toString(), ((NumericalStatisticColumn) v).getMax().toString() );
                        } else {
                            numericalInformation.addRow( k, "❌", "❌" );
                        }

                    } else {
                        alphabeticalInformation.addRow( k, ((AlphabeticStatisticColumn) v).getUniqueValues().toString() );
                    }

                } );
            }
        }, 5000, 5000 );

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

                if ( columns.containsKey( s.getColumnName() ) ) {
                    columns.get( s.getColumnName() ).setUpdated( false );
                    columnsToUpdate.put( s.getColumnName(), columns.get( s.getColumnName() ).getType() );
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
    public void sync() {

        if(storeOutOfSync){
            reevaluateStore();
            storeOutOfSync = false;
        }else {
            columnsToUpdate.forEach( ( column, type ) -> {
                columns.remove( column );
                reevaluateColumn( new QueryColumn( column, type ) );
            } );
        }

        columnsToUpdate.clear();
    }


    @Override
    public void run() {

    }


    private static class StatisticsStoreWorker implements BackgroundTask {


        @Override
        public void backgroundTask() {
            StatisticsStore.getInstance().sync();
        }
    }
}
