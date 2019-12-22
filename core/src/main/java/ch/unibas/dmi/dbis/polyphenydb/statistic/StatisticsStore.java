package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.TransactionStat;
import ch.unibas.dmi.dbis.polyphenydb.TransactionStatType;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGroup;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationTable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Timer;
import java.util.TimerTask;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * Stores all available statistics and updates them dynamically
 */
@Slf4j
public class StatisticsStore<T extends Comparable<T>> implements Observer {

    private static StatisticsStore instance = null;
    private ObservableQueue observableQueue = new ObservableQueue();

    @Getter
    public HashMap<String, StatisticColumn> columns;

    private LowCostQueries sqlQueryInterface;


    private StatisticsStore() {
        this.columns = new HashMap<>();
        displayInformation();

        observableQueue.run();
    }


    public static StatisticsStore getInstance() {
        // To ensure only one instance is created
        if ( instance == null ) {
            instance = new StatisticsStore();
        }
        return instance;
    }


    private void add( String schemaTableColumn, T val ) {
        String[] splits = QueryColumn.getSplitColumn( schemaTableColumn );
        add( splits[0], splits[1], splits[2], val );
    }


    /**
     * adds data for a column to the store
     *
     * @param schema schema name
     * @param table table name in form [schema].[table]
     * @param column column name in form [schema].[table].[column]
     */
    private void add( String schema, String table, String column, T val ) {
        if ( !this.columns.containsKey( column ) ) {
            //PolySqlType type = this.sqlQueryInterface.getColumnType( schema, table, column );
            //log.error(type.toString());
            // TODO: find a solution without race
            PolySqlType type = PolySqlType.INTEGER;
            this.columns.put( column, new AlphabeticStatisticColumn( observableQueue, schema, table, column, type, val ) );
        } else {
            this.columns.get( column ).put( val );
        }
    }


    private void add( String schema, String table, String column, PolySqlType type, T val ) {
        // TODO: switch back to {table = [columns], table = [columns]}
        if ( !this.columns.containsKey( column ) ) {
            this.columns.put( column, new AlphabeticStatisticColumn( observableQueue, schema, table, column, type, val ) );
        } else {
            this.columns.get( column ).put( val );
        }
    }


    public void addAll( String schema, String table, String column, PolySqlType type, List<T> vals ) {
        vals.forEach( val -> {
            // still not sure if generic or not
            add( schema, table, column, type, val );
        } );
    }


    public void remove( String schema, String table, String column, T val ) {
        this.columns.get( column ).remove( val );
    }


    /**
     * Reset all statistics and reevaluate them
     */
    public void reevaluateStore() {
        this.columns.clear();

        for ( QueryColumn column : this.sqlQueryInterface.getAllColumns() ) {
            System.out.println( column.getFullName() );
            if ( column.getType().isNumericalType() ) {
                this.reevaluateNumericalColumn( column );
            } else if ( column.getType().isCharType() ) {
                this.reevaluateAlphabeticalColumn( column );
            }


        }

    }


    private void reevaluteTable( String tableName ) {

    }


    private void reevaluateNumericalColumn( QueryColumn column ) {
        log.error( "reval numerical" );
        log.error( column.getFullName() );
        StatResult min = this.getAggregateColumn( column, "MIN" );
        StatResult max = this.getAggregateColumn( column, "MAX" );
        NumericalStatisticColumn<Integer> statisticColumn = new NumericalStatisticColumn<>( observableQueue, QueryColumn.getSplitColumn( column.getFullName() ) );
        // TODO: rewrite -> change StatisticColumn to use cache
        statisticColumn.setMin( StatResult.toOccurrenceMap( min ) );
        statisticColumn.setMax( StatResult.toOccurrenceMap( max ) );
        log.error( "hai" );
        this.columns.put( column.getFullName(), statisticColumn );
        log.error("addded here " + column.getFullName());
    }


    private void reevaluateAlphabeticalColumn( QueryColumn column ) {
        log.error( "reval alpha" );
        log.error( column.getFullName() );
        StatResult unique = this.getUniqueValues( column );

        StatisticColumn<String> statisticColumn = new AlphabeticStatisticColumn<>( observableQueue, QueryColumn.getSplitColumn( column.getFullName() ) );
        // TODO: rewrite -> change StatisticColumn to use cache
        statisticColumn.putAll( Arrays.asList( unique.getColumns()[0].getData() ) );
        this.columns.put( column.getFullName(), statisticColumn );
    }


    /**
     * Method to get a generic Aggregate Stat with its occurrences
     * TODO: more like min and max atm
     *
     * @return a StatResult which contains the values and their occurences
     */
    private StatResult getAggregateColumn( QueryColumn column, String aggregate ) {
        String order = "ASC";
        if ( aggregate.equals( "MAX" ) ) {
            order = "DESC";
        }
        return this.sqlQueryInterface.selectMultipleStats( "SELECT " + column.getFullName() + ", count(" + column.getFullName() + ") FROM " + column.getFullTableName() + " group BY " + column.getFullName() + " ORDER BY " + column.getFullName() + " " + order );
    }


    private StatResult getUniqueValues( QueryColumn column ) {
        return this.sqlQueryInterface.selectMultipleStats( "SELECT " + column.getFullName() + ", count(" + column.getFullName() + ") FROM " + column.getFullTableName() + " group BY " + column.getFullName() + " ORDER BY " + column.getFullName() );
    }


    public void setSqlQueryInterface( LowCostQueries lowCostQueries ) {
        this.sqlQueryInterface = lowCostQueries;

        this.reevaluateStore();

    }


    @Override
    public void update( Observable o, Object arg ) {
        log.debug( arg.toString() );
    }


    public void displayInformation() {
        InformationManager im = InformationManager.getInstance();

        // TODO: only testwise replace with actual changing data later
        InformationPage page = new InformationPage( "statistics", "Statistics" );
        im.addPage( page );

        InformationGroup contentGroup = new InformationGroup( page, "Column Statistic Status" );
        im.addGroup( contentGroup );

        InformationTable statisticsInformation = new InformationTable( contentGroup, Arrays.asList( "Column Name", "Type", "Updated" ) );

        columns.forEach( ( k, v ) -> {
            statisticsInformation.addRow( k, v.getType().toString(), Boolean.toString( v.isUpdated() ) );
        } );

        im.registerInformation( statisticsInformation );

        Timer t2 = new Timer();
        t2.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                statisticsInformation.reset();
                columns.forEach( ( k, v ) -> {
                    if ( v.isUpdated() ) {
                        statisticsInformation.addRow( k, v.getType().name(), "✔" );
                    } else {
                        statisticsInformation.addRow( k, v.getType().name(), "❌" );
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
                        numericalInformation.addRow( k, ((NumericalStatisticColumn) v).min().toString(), ((NumericalStatisticColumn) v).max().toString() );
                    } else {
                        alphabeticalInformation.addRow( k, ((AlphabeticStatisticColumn) v ).getUniqueValues().keySet().toString());
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
                add( s.getSchema(), s.getTableName(), s.getColumnName(), (T) s.getData() );
            } else if ( type == TransactionStatType.DELETE ) {
                // TODO: implement
            } else if ( type == TransactionStatType.UPDATE ) {
                // TODO: implement
            }
        } );
    }

}
