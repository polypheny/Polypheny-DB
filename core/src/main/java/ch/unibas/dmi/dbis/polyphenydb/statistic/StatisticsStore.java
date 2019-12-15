package ch.unibas.dmi.dbis.polyphenydb.statistic;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


/**
 * Stores all available statistics  and updates them dynamically
 */
public class StatisticsStore {

    private static StatisticsStore instance = null;

    // TODO private again
    public HashMap<String, StatisticColumn> store;

    private String databaseName = "APP";
    private String userName = "pa";
    // private SqlQueryInterface sqlQuery;


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


    }


    public void reevaluteColumn() {

    }


    public void reevaluteStat() {

    }

    /*
    public String[] getMinValues() {
        return this.sqlQuery.getLowCostQueries().selectOneStat(
                "SELECT MIN(public.depts.deptno) FROM public.depts GROUP BY public.depts.deptno ORDER BY MIN(public.depts.deptno) "
        ).getData();
    }


    public void setSqlQueryInterface( SqlQueryInterface sqlQuery ) {
        this.sqlQuery = sqlQuery;
        System.out.println( "got values" );
        System.out.println( Arrays.toString( getMinValues() ) );
    }*/

    public HashMap<String, StatisticColumn> getStore() {
        return this.store;
    }


    interface numericalList extends List<Integer> {

    }


    interface stringList extends List<String> {

    }


    ;

}
