package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.TransactionException;
import ch.unibas.dmi.dbis.polyphenydb.TransactionManager;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.*;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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


    public HashMap<String, StatisticColumn> getStore() {
        return this.store;
    }


    interface numericalList extends List<Integer> {

    }


    interface stringList extends List<String> {

    }


    ;

}
