package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.statistic.model.LimitedOccurrenceMap;
import com.google.gson.annotations.Expose;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * Stores the available statistic data of a specific column
 * Responsible to validate if data should be changed
 */
@Slf4j
public class NumericalStatisticColumn<T extends Comparable<T>> extends Observable implements StatisticColumn<T> {

    @Getter
    private final String schema;
    @Getter
    private final String table;
    @Getter
    private final String column;

    @Expose
    @Getter
    private LimitedOccurrenceMap<T> uniqueValues = new LimitedOccurrenceMap<>( Comparator.reverseOrder() );

    private LimitedOccurrenceMap<T> minCache = new LimitedOccurrenceMap<>( );
    private LimitedOccurrenceMap<T> maxCache = new LimitedOccurrenceMap<>( Comparator.reverseOrder());

    @Getter
    //TODO: add
    private PolySqlType type = PolySqlType.INTEGER;

    @Expose
    @Getter
    private T min;
    @Expose
    @Getter
    private T max;

    @Expose
    @Getter
    private int count;

    @Expose
    private boolean isFull;

    @Getter
    private boolean updated = true;


    private NumericalStatisticColumn( Observer observer, String schema, String table, String column, T val ) {
        this( observer, schema, table, column );
        put( val );
    }


    public NumericalStatisticColumn( Observer observer, String schema, String table, String column, PolySqlType type, T val ) {
        this( observer, schema, table, column, val );
        this.type = type;
    }


    public NumericalStatisticColumn( Observer observer, String schema, String table, String column ) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.addObserver( observer );
    }


    public NumericalStatisticColumn( Observer observer, String[] splitColumn, PolySqlType type ) {
        this( observer, splitColumn[0], splitColumn[1], splitColumn[2] );
        this.type = type;
    }


    /**
     * check for potential "recordable data"
     */
    @Override
    public void put( T val ) {
        this.uniqueValues.put( val );
        this.maxCache.put( val );
        this.minCache.put( val );

        resetMin();
        resetMax();

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


    @Override
    public void putAll( List<T> vals ) {
        vals.forEach( this::put );
    }


    private void resetMin() {
        min = this.minCache.firstKey();
    }


    private void resetMax() {
        max = this.maxCache.firstKey();
    }


    @Override
    public String toString() {
        String stats = "";

        stats += "min: " + min;
        stats += "max: " + max;
        stats += "count: " + count;
        stats += "unique Value: " + uniqueValues.toString();

        return stats;

    }


    /**
     * Remove should check if value occurs in list and decrease its occurence by 1
     * If it occurs 0 times after, it gets removed
     *
     * @param val the value which is removed
     */
    @Override
    public void remove( T val ) {
        this.minCache.remove( val );
        this.maxCache.remove( val );
        this.uniqueValues.remove( val );

        if ( this.minCache.isEmpty() || this.maxCache.isEmpty() || this.uniqueValues.isEmpty() ) {
            setChanged();
            notifyObservers( column );
        }

    }

    @Override
    public void removeAll( List<T> vals) {
        vals.forEach( this::remove );
    }
}
