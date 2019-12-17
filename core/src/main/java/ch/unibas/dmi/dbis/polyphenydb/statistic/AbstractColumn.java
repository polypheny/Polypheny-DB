package ch.unibas.dmi.dbis.polyphenydb.statistic;


import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


/**
 * Boilerplate of a column to guide the handling and pattern of a column
 */
@Slf4j
abstract class AbstractColumn {

    @Getter
    private String schema;

    @Getter
    private String table;

    @Getter
    private String name;

    /**
     * Type of the column
     */
    @Getter
    private String type;

    AbstractColumn(String schema, String table, String name, String type){
        this.schema = schema;
        this.table = table;
        this.name = name;
        this.type = type;
    }

    AbstractColumn(String schemaTableName, String type) {
        this(schemaTableName.split( "\\." )[0], schemaTableName.split( "\\." )[1], schemaTableName.split( "\\." )[2], type);
    }
}
