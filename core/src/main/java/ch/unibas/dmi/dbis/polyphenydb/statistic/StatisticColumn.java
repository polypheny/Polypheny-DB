package ch.unibas.dmi.dbis.polyphenydb.statistic;


import java.util.HashMap;
import java.util.List;
import java.util.Observable;
import java.util.SortedMap;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * Stores the available statistic data of a specific column
 * Responsible to validate if data should be changed
 */
@Slf4j
public class StatisticColumn<T extends Comparable<T>> extends Observable {

    private final int MAX_MOST_USED_WORDS = 5;
    private final String id;

    @Setter
    private SortedMap<T, Integer> minCache;
    @Setter
    private T min;

    @Setter
    private SortedMap<T, Integer> maxCache;
    @Setter
    private T max;

    private HashMap<T, Integer> uniqueValues = new HashMap<>();
    private boolean isFull;


    public StatisticColumn( String id, T val ) {
        this( id );
        put( val );
    }


    public StatisticColumn( String id ) {
        this.id = id;
    }


    /**
     * check for potential "recordable data"
     */
    public void put( T val ) {
        updateMinMax( val );

        if ( !isFull ) {
            addUnique( val );
        }
    }


    public void putAll( List<T> vals ) {
        vals.forEach( this::put );
    }


    private void addUnique( T val ) {
        if ( uniqueValues.containsKey( val ) ) {
            uniqueValues.put( val, uniqueValues.get( val ) + 1 );
        } else if ( !isFull && uniqueValues.size() < MAX_MOST_USED_WORDS ) {
            uniqueValues.put( val, 1 );
            if ( uniqueValues.size() > MAX_MOST_USED_WORDS ) {
                isFull = true;
            }
        } else {
            return;
        }
        //log.info(" updated addUnique " + val.toString());
    }


    private void updateMinMax( T val ) {
        // just for safety, might delete later...
        if ( val instanceof String ) {
            return;
        }


    }

}
