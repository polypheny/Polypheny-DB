package ch.unibas.dmi.dbis.polyphenydb.statistic.model;


import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;


public class SortedNumericalMap implements SortedMap {

    SortedNumericalMap(Aggregate aggregate){

    }

    @Override
    public Comparator comparator() {
        return Comparator.naturalOrder();
    }


    @Override
    public SortedMap subMap( Object fromKey, Object toKey ) {
        return null;
    }


    @Override
    public SortedMap headMap( Object toKey ) {
        return null;
    }


    @Override
    public SortedMap tailMap( Object fromKey ) {
        return null;
    }


    @Override
    public Object firstKey() {
        return null;
    }


    @Override
    public Object lastKey() {
        return null;
    }


    @Override
    public int size() {
        return 0;
    }


    @Override
    public boolean isEmpty() {
        return false;
    }


    @Override
    public boolean containsKey( Object key ) {
        return false;
    }


    @Override
    public boolean containsValue( Object value ) {
        return false;
    }


    @Override
    public Object get( Object key ) {
        return null;
    }


    @Override
    public Object put( Object key, Object value ) {
        return null;
    }


    @Override
    public Object remove( Object key ) {
        return null;
    }


    @Override
    public void putAll( Map m ) {

    }


    @Override
    public void clear() {

    }


    @Override
    public Set keySet() {
        return null;
    }


    @Override
    public Collection values() {
        return null;
    }


    @Override
    public Set<Entry> entrySet() {
        return null;
    }
}
