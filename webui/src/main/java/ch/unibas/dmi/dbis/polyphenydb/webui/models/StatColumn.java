package ch.unibas.dmi.dbis.polyphenydb.webui.models;


import java.util.HashMap;
import lombok.Getter;


/**
 * Contains stat data for a column
 * TODO: "combine" with Result model through interface...
 */
public class StatColumn {

    /**
     * Type of the column
     */
    @Getter
    private String type;

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
    public StatColumn( final String type, final String[] data ) {
        this.type = type;
        this.data = data;
    }

}
