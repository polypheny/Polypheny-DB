package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigManager;
import com.google.gson.annotations.Expose;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;


/**
 * Stores the available statistic data of a specific column
 * If is responsible to ask for a update if it falls out of "sync"
 */

public abstract class StatisticColumn<T extends Comparable<T>> {
    @Getter
    private final String schema;
    @Getter
    private final String table;
    @Getter
    private final String column;
    @Getter
    private final PolySqlType type;

    @Expose
    private final String fullColumnName;

    @Expose
    @Setter
    boolean isFull;

    @Expose
    @Getter
    @Setter
    public List<T> uniqueValues = new ArrayList<>();


    @Expose
    @Getter
    @Setter
    public int count;


    @Getter
    @Setter
    private boolean updated = true;


    public StatisticColumn( String schema, String table, String column, PolySqlType type ) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.type = type;
        this.fullColumnName = schema + "." + table + "." + column;
    }


    public StatisticColumn( String[] splitColumn, PolySqlType type ) {
        this( splitColumn[0], splitColumn[1], splitColumn[2], type );
    }


    public abstract void insert( T val );


    public abstract String toString();
}
