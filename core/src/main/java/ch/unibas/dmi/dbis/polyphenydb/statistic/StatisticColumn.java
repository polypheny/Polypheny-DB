package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import java.util.List;


/**
 * Stores the available statistic data of a specific column
 * Responsible to validate if data should be changed
 */

public interface StatisticColumn<T> {


    /**
     * insert possible valuable information into the column
     */
    void put( T val );

    void putAll( List<T> vals );

    String toString();

    void remove( T val );

    void removeAll( List<T> vals );

    PolySqlType getType();

    boolean isUpdated();
}
