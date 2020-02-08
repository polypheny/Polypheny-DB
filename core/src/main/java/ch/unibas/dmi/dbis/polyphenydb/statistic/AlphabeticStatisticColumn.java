package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigManager;
import com.google.gson.annotations.Expose;
import lombok.extern.slf4j.Slf4j;


/**
 * Stores the available statistic data of a specific column
 * Responsible to validate if data should be changed
 */
@Slf4j
public class AlphabeticStatisticColumn<T extends Comparable<T>> extends StatisticColumn<T> {

    @Expose
    private final String columnType = "alphabetic";


    public AlphabeticStatisticColumn( String schema, String table, String column, PolySqlType type ) {
        super( schema, table, column, type );
    }


    public AlphabeticStatisticColumn( String[] splitColumn, PolySqlType type ) {
        super( splitColumn[0], splitColumn[1], splitColumn[2], type );
    }


    @Override
    public void insert( T val ) {
        if ( uniqueValues.size() < ConfigManager.getInstance().getConfig( "StatisticColumnBuffer" ).getInt() ) {
            if ( ! uniqueValues.contains( val ) ) {
                uniqueValues.add( val );
            }
        } else {
            isFull = true;
        }
    }


    @Override
    public String toString() {
        String statistics = "";

        statistics += "count: " + count;
        statistics += "unique Value: " + uniqueValues.toString();

        return statistics;

    }

}
