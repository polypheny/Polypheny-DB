package ch.unibas.dmi.dbis.polyphenydb.statistic;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import lombok.extern.slf4j.Slf4j;


/**
 * Stores all available statistics  and updates them dynamically
 */
@Slf4j
public class StatisticsStore implements Observer {

    private static StatisticsStore instance = null;

    // TODO private again
    public HashMap<String, StatisticColumn> store;

    private String databaseName = "APP";
    private String userName = "pa";
    private LowCostQueries sqlQueryInterface;


    private StatisticsStore() {
        this.store = new HashMap<>();
        this.mockContent();
    }


    public static StatisticsStore getInstance() {
        // To ensure only one instance is created
        if ( instance == null ) {
            instance = new StatisticsStore();
        }
        return instance;
    }


    public HashMap<String, StatisticColumn> getStore() {
        return this.store;
    }


    private void mockContent() {
        this.update( "public.depts", "deptno", 3 );
        this.update( "public.depts", "deptno", 10 );

        this.update( "public.depts", "name", "tester1" );
        this.update( "public.depts", "name", "tester10" );
        this.update( "public.depts", "name", "tester100" );

        this.update( "public.emps", "name", "tester10" );
    }


    public void update( String table, String column, int val ) {
        if ( !this.store.containsKey( table ) ) {
            this.store.put( table, new StatisticColumn( column, val ) );
        } else {
            this.store.get( table ).put( val );
        }
    }


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
        if ( !this.store.containsKey( table ) ) {
            this.store.put( table, new StatisticColumn( column, val ) );
        } else {
            this.store.get( table ).put( val );
        }
    }


    /**
     * Reset all statistics and reevaluate them
     */
    public void reevaluateStore() {
        this.store.clear();

        for ( String column : this.sqlQueryInterface.getAllColumns() ) {
            // TODO: cant min and max for varchar
            this.reevaluteColumn( column );

        }

    }


    private void reevaluteTable( String tableName ) {

    }


    private void reevaluteColumn( String columnName ) {
        String[] splits = columnName.split( "\\." );
        System.out.println( columnName );
        StatResult min = this.getAggregateColumn( columnName, splits[0] + "." + splits[1], "MIN" );
        StatResult max = this.getAggregateColumn( columnName, splits[0] + "." + splits[1], "MAX" );

        Arrays.asList( min.getColumns()[0].getData() ).forEach( System.out::println );
        System.out.println( "max" );
        Arrays.asList( max.getColumns()[0].getData() ).forEach( System.out::println );
    }


    /**
     * Method to get a generic Aggregate Stat with its occurrences
     *
     * @return a StatResult which contains the values and their occurences
     */
    private StatResult getAggregateColumn( String column, String table, String aggregate ) {
        return this.sqlQueryInterface.selectMultipleStats( "SELECT " + aggregate + "(" + column + "), COUNT(" + column + ") FROM " + table + " GROUP BY " + column + " ORDER BY " + aggregate + "(" + column + ") " );
    }


    public void setSqlQueryInterface( LowCostQueries lowCostQueries ) {
        this.sqlQueryInterface = lowCostQueries;
        //StatColumn res = this.sqlQueryInterface.selectOneStat( "SELECT MIN(public.depts.deptno) FROM public.depts GROUP BY public.depts.deptno ORDER BY MIN(public.depts.deptno) " );

        /*System.out.println( Arrays.toString( res.getData() ) );
        System.out.println( res.getType() );*/
        this.reevaluateStore();

    }


    @Override
    public void update( Observable o, Object arg ) {
        log.debug( arg.toString() );
    }


    interface numericalList extends List<Integer> {

    }


    interface stringList extends List<String> {

    }

}
