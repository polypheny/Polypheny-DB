package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import com.google.gson.annotations.Expose;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;


/**
 * Stores the available statistic data of a specific column
 */
public abstract class StatisticColumn<T> {

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
    private final String qualifiedColumnName;

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


    public StatisticColumn( String schema, String table, String column, PolySqlType type ) {
        this.schema = schema.replace( "\\", "" ).replace( "\"", "" );
        this.table = table.replace( "\\", "" ).replace( "\"", "" );
        this.column = column.replace( "\\", "" ).replace( "\"", "" );
        this.type = type;
        this.qualifiedColumnName = this.schema + "." + this.table + "." + this.column;

    }


    StatisticColumn( String[] splitColumn, PolySqlType type ) {
        this( splitColumn[0], splitColumn[1], splitColumn[2], type );
    }


    public String getQualifiedTableName() {
        return this.schema + "." + this.table;
    }


    public abstract void insert( T val );

    public abstract String toString();
}
