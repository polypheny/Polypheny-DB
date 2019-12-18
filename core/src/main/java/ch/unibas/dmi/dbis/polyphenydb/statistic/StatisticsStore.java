package ch.unibas.dmi.dbis.polyphenydb.statistic;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import lombok.extern.slf4j.Slf4j;
import org.pentaho.aggdes.model.Aggregate;


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
        statisticColumn.setMin( Integer.parseInt( min.getColumns()[0].getData()[0] ) );
        statisticColumn.setMax( Integer.parseInt( max.getColumns()[0].getData()[0] ) );
        this.store.put( column.getFullName(), statisticColumn );
    }


    private void reevaluateAlphabeticalColumn( QueryColumn column ) {
        StatResult unique = this.getUniqueValues( column );

        StatisticColumn<String> statisticColumn = new StatisticColumn<>( column.getFullName() );
        // TODO: rewrite -> change StatisticColumn to use cache
        statisticColumn.putAll( Arrays.asList( unique.getColumns()[0].getData() ) );

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


    interface numericalList extends List<Integer> {

    }


    interface stringList extends List<String> {

    }

}
