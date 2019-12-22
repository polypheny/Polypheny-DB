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
public class AlphabeticStatisticColumn<T extends Comparable<T>, S> extends Observable implements StatisticColumn<T> {

    @Getter
    private final String schema;
    @Getter
    private final String table;
    @Getter
    private final String column;

    @Expose
    @Getter
    private LimitedOccurrenceMap<T> uniqueValues = new LimitedOccurrenceMap<>( Comparator.reverseOrder() );


    @Getter
    //TODO: add
    private PolySqlType type = PolySqlType.INTEGER;

    @Expose
    @Getter
    private int count;

    @Expose
    private boolean isFull;

    @Getter
    private boolean updated = true;


    private AlphabeticStatisticColumn( Observer observer, String schema, String table, String column, T val ) {
        this( observer, schema, table, column );
        put( val );
    }


    public AlphabeticStatisticColumn( Observer observer, String schema, String table, String column, PolySqlType type, T val ) {
        this( observer, schema, table, column, val );
        this.type = type;
    }


    public AlphabeticStatisticColumn( Observer observer, String schema, String table, String column ) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.addObserver( observer );
    }


    public AlphabeticStatisticColumn( Observer observer, String[] splitColumn ) {
        this( observer, splitColumn[0], splitColumn[1], splitColumn[2] );
    }


    /**
     * check for potential "recordable data"
     */
    @Override
    public void put( T val ) {
        this.uniqueValues.put( val );

    }


    @Override
    public void putAll( List<T> vals ) {
        vals.forEach( this::put );
    }


    @Override
    public String toString() {
        String stats = "";

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
        this.uniqueValues.remove( val );

        if ( this.uniqueValues.isEmpty() ) {
            setChanged();
            notifyObservers( column );
        }

    }


    @Override
    public void removeAll( List<T> vals ) {
        vals.forEach( this::remove );
    }
}
