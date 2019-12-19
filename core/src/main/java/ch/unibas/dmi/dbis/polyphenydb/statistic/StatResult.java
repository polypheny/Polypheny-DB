package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;


/**
 * Contains stats for multiple columns  TODO: evaluate need
 */
public class StatResult {

    @Getter
    private StatQueryColumn[] columns;
    @Getter
    private String[] columnNames;


    public StatResult( StatQueryColumn[] columns ) {
        this.columns = columns;
    }


    /**
     * Constructor which transforms an answer-array into multiple statcolums
     *
     * @param data answer per stat as a two-dimensional array
     */
    public StatResult( ArrayList<String> names, ArrayList<PolySqlType> type, String[][] data ) {

        this.columns = new StatQueryColumn[data[0].length];

        String[][] rotated = rotate2dArray( data );

        for ( int i = 0; i < rotated.length; i++ ) {
            this.columns[i] = new StatQueryColumn( names.get( i ), type.get( i ), rotated[i] );
        }

    }


    /**
     * Rotates a 2d array counterclockwise
     * Assumes 2d array is equally long in all "sub"arrays
     */
    private String[][] rotate2dArray( String[][] data ) {
        int width = data[0].length;
        int height = data.length;

        String[][] rotated = new String[width][height];

        for ( int x = 0; x < width; x++ ) {
            for ( int y = 0; y < height; y++ ) {
                rotated[x][y] = data[y][x];
            }
        }
        return rotated;
    }

    /**
     * Transforms an StatResult, which has to consist of <b>value</b> and <b>occurrence</b> of a column, into a map
     *
     * @return map with <b>value</b> as key and <b>occurrence</b> as value
     */
    public static <E> Map<E, Integer> toOccurrenceMap( StatResult stats ) {
        HashMap<E, Integer> map = new HashMap();
        String[] values = stats.getColumns()[0].getData();
        String[] occurrences = stats.getColumns()[1].getData();
        //TODO: handle missmatch
        for ( int i = 0; i < values.length; i++ ) {
            map.put( (E) values[i], Integer.parseInt( occurrences[i] ) );
        }
        return map;
    }
}
