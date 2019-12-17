package ch.unibas.dmi.dbis.polyphenydb.statistic;


import lombok.Getter;


/**
 * Contains stat data for a column
 * TODO: "combine" with Result model through interface...
 */
public class StatColumn extends AbstractColumn{


    /**
     * all specified stats for a column identified by their keys
     */
    @Getter
    private String[] data;


    /**
     * Builds a StatColumn with the individual stats of a column
     * @param type db type of the column
     * @param data map consisting of different values to a given stat
     */
    public StatColumn( String schemaTableName, final String type, final String[] data ) {
        super(schemaTableName, type);
    }

    public StatColumn( String schema, String table, String name, final String type, final String[] data){
        super(schema, table, name, type );
    }

}
