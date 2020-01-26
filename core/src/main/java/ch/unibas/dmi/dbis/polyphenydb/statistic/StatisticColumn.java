package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.config.ConfigManager;
import com.google.gson.annotations.Expose;
import lombok.Getter;
import lombok.Setter;


/**
 * Stores the available statistic data of a specific column
 * If is responsible to ask for a update if it falls out of "sync"
 */

public abstract class StatisticColumn<T extends Comparable<T>> {
    @Expose
    @Getter
    private final String schema;
    @Expose
    @Getter
    private final String table;
    @Expose
    @Getter
    private final String column;
    @Getter
    private final PolySqlType type;


    @Expose
    @Getter
    @Setter
    public int count;

    @Getter
    private int listBufferSize = ConfigManager.getInstance().getConfig( "StatisticsPerColumn" ).getInt();


    @Getter
    @Setter
    private boolean updated = true;


    public StatisticColumn( String schema, String table, String column, PolySqlType type ) {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.type = type;
    }


    public StatisticColumn( String[] splitColumn, PolySqlType type ) {
        this( splitColumn[0], splitColumn[1], splitColumn[2], type );
    }


    public abstract void insert( T val );


    public abstract String toString();
}
