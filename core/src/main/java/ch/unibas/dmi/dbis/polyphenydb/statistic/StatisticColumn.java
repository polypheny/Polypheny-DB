package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.statistic.model.LimitedOccurrenceMap;
import com.google.gson.annotations.Expose;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * Stores the available statistic data of a specific column
 * Responsible to validate if data should be changed
 */
@Slf4j
public class StatisticColumn<T extends Comparable<T>> extends Observable {

    private final int MAX_MOST_USED_WORDS = 5;
    private final int CACHE_SIZE = 5;
    private final String id;

    private LimitedOccurrenceMap<T> minCache = new LimitedOccurrenceMap<T>( CACHE_SIZE );
    private LimitedOccurrenceMap<T> maxCache = new LimitedOccurrenceMap<T>( Comparator.reverseOrder(), CACHE_SIZE );

    @Expose
    private T min;
    @Expose
    private T max;

    @Expose
    private float count;
    @Expose
    private HashMap<T, Integer> uniqueValues = new HashMap<>();
    @Expose
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


    public T max() {
        return this.maxCache.firstKey();
    }


    public T min() {
        return this.minCache.firstKey();
    }


    /**
     * reinitialize minCache
     */
    public void setMin( Map<T, Integer> map ) {
        this.minCache.setAll( map );
        //TODO: handle streamlined
        resetMin();
    }


    /**
     * reinitialize maxCache
     */
    public void setMax( Map<T, Integer> map ) {
        this.maxCache.setAll( map );
        //TODO: handle streamlined
        resetMax();
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
        }
    }


    private void updateMinMax( T val ) {
        // just for safety, might delete later...
        if ( val instanceof String ) {
            return;
        }
        this.maxCache.put( val );
        this.minCache.put( val );

        resetMin();
        resetMax();
    }

    private void resetMin() {
        min = this.minCache.firstKey();
    }

    private void resetMax() {

        max = this.maxCache.firstKey();
    }

}
