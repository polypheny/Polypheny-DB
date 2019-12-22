package ch.unibas.dmi.dbis.polyphenydb.statistic.model;


import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.extern.slf4j.Slf4j;


/**
 * Special implementation of a TreeMap to only allow a regulated size of entries
 */
@Slf4j
public
class LimitedOccurrenceMap<K> {

    private int maxSize;

    private TreeMap<K, Integer> map;


    public LimitedOccurrenceMap( int maxSize ) {
        this.map = new TreeMap<>();
        this.maxSize = maxSize;
    }


    public LimitedOccurrenceMap( Comparator<? super K> comparator, int maxSize ) {
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

        while ( map.size() > maxSize ) {
            map.remove( map.lastKey() );
        }

    }


    public K firstKey() {
        return this.map.firstKey();
    }


    public void setAll( Map<K, Integer> map ) {
        this.map.clear();

        map.forEach( ( k, v ) -> {
            this.map.put( k, v );
        } );

        while ( this.map.size() > maxSize ) {
            map.remove( this.map.lastKey() );
        }

    }


    /**
     * Method changes the occurrence counter of a given value by a specified amount if it exists
     *
     * @param key the value which gets changed
     */
    public void remove( K key ) {
        if ( this.map.containsKey( key ) ) {
            if ( this.map.get( key ) - 1 == 0 ) {
                this.map.remove( key );
            } else {
                this.map.replace( key, this.map.get( key ) - 1 );
            }
        }
    }

    public int size(){
        return this.map.size();
    }


    public boolean isEmpty() {
        return map.size() == 0;
    }

}
