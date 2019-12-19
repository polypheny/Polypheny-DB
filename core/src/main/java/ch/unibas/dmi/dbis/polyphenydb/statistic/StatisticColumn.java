package ch.unibas.dmi.dbis.polyphenydb.statistic;


import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.SortedMap;
import java.util.TreeMap;
import lombok.Setter;
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
    @Setter
    private T min;

    private LimitedOccurrenceMap<T> maxCache = new LimitedOccurrenceMap<T>( Comparator.reverseOrder(), CACHE_SIZE );
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
        this.maxCache.put( val );
        this.minCache.put( val );
    }


    /**
     * Special implementation of a TreeMap to only allow a regulated size of entries
     */
    class LimitedOccurrenceMap<K> {

        private int maxSize;
        private TreeMap<K, Integer> map;


        LimitedOccurrenceMap( int maxSize ) {
            this.map = new TreeMap<>();
            this.maxSize = maxSize;
        }


        LimitedOccurrenceMap( Comparator<? super K> comparator, int maxSize ) {
            this.map = new TreeMap<>( comparator );
            this.maxSize = maxSize;
        }


        public void put( K key ) {
            if ( map.containsKey( key ) ) {
                map.replace( key, map.get( key ) );
            } else {
                map.put( key, 1 );
            }
            if ( map.size() > maxSize ) {
                map.remove( map.firstKey() );
            }
        }


        public void putAll( List<K> keys ) {
            keys.forEach( k -> {
                if ( map.containsKey( k ) ) {
                    map.replace( k, map.get( k ) );
                } else {
                    map.put( k, 1 );
                }
            } );

            if ( map.size() > maxSize ) {
                while(map.size() > maxSize){
                    map.remove( map.lastKey() );
                }
            }
        }

    }

}
