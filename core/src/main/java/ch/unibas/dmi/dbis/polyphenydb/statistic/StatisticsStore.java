package ch.unibas.dmi.dbis.polyphenydb.statistic;


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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * Stores all available statistics  and updates them dynamically
 */
@Slf4j
public class StatisticsStore implements Observer {

    private static StatisticsStore instance = null;
    private ObservableQueue observableQueue = new ObservableQueue();

    @Getter
    public HashMap<String, StatisticColumn> columns;

    private LowCostQueries sqlQueryInterface;


    private StatisticsStore() {
        this.columns = new HashMap<>();
        //this.mockContent();
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


    private void mockContent() {
        this.update( "public.depts", "deptno", 3 );
        this.update( "public.depts", "deptno", 10 );

        this.update( "public.depts", "name", "tester1" );
        this.update( "public.depts", "name", "tester10" );
        this.update( "public.depts", "name", "tester100" );

        this.update( "public.emps", "name", "tester10" );
    }

    //TODO: Rename
    public void update( String table, String column, int val ) {
        if ( !this.columns.containsKey( table ) ) {
            this.columns.put( table, new StatisticColumn( column, val ) );
        } else {
            this.columns.get( table ).put( val );
        }
    }

    //TODO: Rename
    public void updateAll( String table, String column, stringList vals ) {
        vals.forEach( val -> {
            // still not sure if generic or not
            update( table, column, val );
        } );
    }


    public void updateAll( String table, String column, numericalList vals ) {
        vals.forEach( val -> {
            // still not sure if generic or not
            update( table, column, val );
        } );
    }


    public void update( String table, String column, String val ) {
        // TODO: switch back to {table = [columns], table = [columns]}
        if ( !this.columns.containsKey( column ) ) {
            this.columns.put( column, new StatisticColumn( column, val ) );
        } else {
            this.columns.get( column ).put( val );
        }
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
        StatResult min = this.getAggregateColumn( column, "MIN" );
        StatResult max = this.getAggregateColumn( column, "MAX" );
        StatisticColumn<Integer> statisticColumn = new StatisticColumn<>( column.getFullName() );
        // TODO: rewrite -> change StatisticColumn to use cache
        statisticColumn.setMin( StatResult.toOccurrenceMap( min ) );
        statisticColumn.setMax( StatResult.toOccurrenceMap( max ) );
        this.columns.put( column.getFullName(), statisticColumn );
    }


    private void reevaluateAlphabeticalColumn( QueryColumn column ) {
        StatResult unique = this.getUniqueValues( column );

        StatisticColumn<String> statisticColumn = new StatisticColumn<>( column.getFullName() );
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

        //SystemInfo si = new SystemInfo();
        //OperatingSystem os = si.getOperatingSystem();

        // TODO: only testwise replace with actual changing data later
        InformationPage page = new InformationPage( "statistics", "Statistics" );
        im.addPage( page );

        InformationGroup explainGroup = new InformationGroup( page, "Statistics per Column" );
        im.addGroup( explainGroup );

        InformationTable explainInformation = new InformationTable( explainGroup, Arrays.asList( "Type", "Explanation" ) );
        explainInformation.addRow( "Min", "Minimal Value" );
        explainInformation.addRow( "Max", "Maximal Value" );
        explainInformation.addRow( "UniqueValue", "Unique Values" );

        im.registerInformation( explainInformation );


        /*
        Timer t2 = new Timer();
        t2.scheduleAtFixedRate( new TimerTask() {
            @Override
            public void run() {
                List<OSProcess> procs = Arrays.asList( os.getProcesses( 5, ProcessSort.CPU ) );

                ArrayList<String> procNames = new ArrayList<>();
                ArrayList<Double> procPerc = new ArrayList<>();
                for ( int i = 0; i < procs.size() && i < 5; i++ ) {
                    OSProcess proc = procs.get( i );
                    double cpuPerc = 100d * (proc.getKernelTime() + proc.getUserTime()) / proc.getUpTime();
                    String name = proc.getName();
                    procNames.add( name );
                    procPerc.add( Math.round( cpuPerc * 10.0 ) / 10.0 );

                }

                GraphData<Double> data2 = new GraphData<>( "processes", procPerc.toArray( new Double[0] ) );
                graph.updateGraph( procNames.toArray( new String[0] ), data2 );
            }
        }, 0, 5000 );*/
    }


    /**
     * Transaction hands it changes so they can be added.
     *
     * @param stats all changes for this store
     */
    public void apply( ArrayList<TransactionStat> stats ) {
        log.error( "applying" );
        stats.forEach( s -> {
            log.error(s.getColumnName());
            TransactionStatType type = s.getType();
                // TODO: better prefiltering
            if ( type == TransactionStatType.INSERT ) {
                update( s.getTableName(), s.getColumnName(), s.getData()  );
            } else if ( type == TransactionStatType.DELETE ) {
                // TODO: implement
            } else {
                // TODO: implement
            }
        } );
    }


    interface numericalList extends List<Integer> {

    }


    interface stringList extends List<String> {

    }

}
