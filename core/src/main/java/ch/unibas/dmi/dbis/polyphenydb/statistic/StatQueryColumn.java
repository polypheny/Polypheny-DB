package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;


/**
 * Contains stat data for a column
 * TODO: "combine" with Result model through interface...
 */
public class StatQueryColumn extends QueryColumn {


    /**
     * all specified stats for a column identified by their keys
     */
    @Getter
    private String[] data;


    /**
     * Builds a StatColumn with the individual stats of a column
     *
     * @param type db type of the column
     * @param data map consisting of different values to a given stat
     */
    public StatQueryColumn( String schemaTableName, final PolySqlType type, final String[] data ) {
        super( schemaTableName, type );
        this.data = data;
    }


    public StatQueryColumn( String schema, String table, String name, final PolySqlType type, final String[] data ) {
        super( schema, table, name, type );
        this.data = data;
    }



}
