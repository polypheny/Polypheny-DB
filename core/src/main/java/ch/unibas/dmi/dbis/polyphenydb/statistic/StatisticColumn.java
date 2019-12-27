package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import com.google.gson.annotations.Expose;
import java.util.Observable;
import java.util.Observer;
import lombok.Getter;


/**
 * Stores the available statistic data of a specific column
 * If is responsible to ask for a update if it falls out of "sync"
 */

public abstract class StatisticColumn<T extends Comparable<T>> extends Observable {

    @Getter
    private final String schema;
    @Getter
    private final String table;
    @Getter
    private final String column;
    @Getter
    private final PolySqlType type;

    @Expose
    @Getter
    public int count;

    @Getter
    private int listBufferSize = 5;


    @Getter
    private boolean updated = true;


    public StatisticColumn( Observer observer, String schema, String table, String column, PolySqlType type ) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.addObserver( observer );
        this.type = type;
    }


    public StatisticColumn( Observer observer, String[] splitColumn, PolySqlType type ) {
        this( observer, splitColumn[0], splitColumn[1], splitColumn[2], type );
    }


    private void requestUpdate() {
        setChanged();
        notifyObservers( column );
    }


    public abstract void insert( T val );


    public abstract String toString();
}
