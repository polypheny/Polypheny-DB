package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import lombok.Getter;


/**
 * Contains stat data for a column
 */
public class StatisticQueryColumn extends QueryColumn {


    /**
     * all specified statistics for a column identified by their keys
     */
    @Getter
    private String[] data;


    /**
     * Builds a StatColumn with the individual stats of a column
     *
     * @param type db type of the column
     * @param data map consisting of different values to a given stat
     */
    public StatisticQueryColumn( String schemaTableName, final PolySqlType type, final String[] data ) {
        super( schemaTableName, type );
        this.data = data;
    }


    public StatisticQueryColumn( String schema, String table, String name, final PolySqlType type, final String[] data ) {
        super( schema, table, name, type );
        this.data = data;
    }


}
