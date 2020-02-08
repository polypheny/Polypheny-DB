package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;


/**
 * Contains statistics for multiple columns
 */
public class StatisticResult {

    @Getter
    private StatisticQueryColumn[] columns;
    @Getter
    private String[] columnNames;


    public StatisticResult() {
    }


    public StatisticResult( StatisticQueryColumn[] columns ) {
        this.columns = columns;
    }


    /**
     * Constructor which transforms an answer-array into multiple StatisticColumns
     *
     * @param data answer per stat as a two-dimensional array
     */
    public StatisticResult( List<String> names, List<PolySqlType> type, String[][] data ) {
        if ( data.length == 0 || data[0].length == 0 ) {
            this.columns = new StatisticQueryColumn[0];
        } else {
            this.columns = new StatisticQueryColumn[data[0].length];

            String[][] rotated = rotate2dArray( data );

            for ( int i = 0; i < rotated.length; i++ ) {
                this.columns[i] = new StatisticQueryColumn( names.get( i ), type.get( i ), rotated[i] );
            }
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
     * Transforms an StatisticResult, which has to consist of <b>value</b> and <b>occurrence</b> of a column, into a map
     *
     * @return map with <b>value</b> as key and <b>occurrence</b> as value
     */
    public static <E> Map<E, Integer> toOccurrenceMap( StatisticResult stats ) {
        HashMap<E, Integer> map = new HashMap<>();
        String[] values = stats.getColumns()[0].getData();
        String[] occurrences = stats.getColumns()[1].getData();
        //TODO: handle mismatch
        for ( int i = 0; i < values.length; i++ ) {
            map.put( (E) values[i], Integer.parseInt( occurrences[i] ) );
        }
        return map;
    }
}
