package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import com.google.gson.annotations.Expose;
import java.util.ArrayList;
import java.util.List;
import java.util.Observer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;


/**
 * Stores the available statistic data of a specific column
 * Responsible to validate if data should be changed
 */
@Slf4j
public class NumericalStatisticColumn<T extends Comparable<T>> extends StatisticColumn<T> {

    @Expose
    @Getter
    @Setter
    private List<T> uniqueValues = new ArrayList<>();

    @Expose
    private final String columnType = "numeric";

    @Expose
    @Getter
    @Setter
    private T min;

    @Expose
    @Getter
    @Setter
    private T max;

    @Expose
    private boolean isFull = false;


    public NumericalStatisticColumn( String schema, String table, String column, PolySqlType type ) {
        super( schema, table, column, type );
    }


    public NumericalStatisticColumn( String[] splitColumn, PolySqlType type ) {
        super( splitColumn, type );
    }


    @Override
    public void insert( T val ) {
        if ( uniqueValues.size() < getListBufferSize() ) {
            if ( !uniqueValues.contains( val ) ) {
                uniqueValues.add( val );
            }
        } else {
            isFull = true;
        }
        if ( min == null ) {
            min = val;
            max = val;
        } else if ( val.compareTo( min ) < 0 ) {
            this.min = val;
        } else if ( val.compareTo( max ) > 0 ) {
            this.max = val;
        }
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


    public void setUnique( List<String> unique ) {

    }


}
