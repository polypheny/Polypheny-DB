package ch.unibas.dmi.dbis.polyphenydb.statistic;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import lombok.Getter;


/**
 * Boilerplate of a column to guide the handling and pattern of a column
 */
class QueryColumn {

    @Getter
    private String schema;

    @Getter
    private String table;

    @Getter
    private String name;

    @Getter
    private PolySqlType type;


    QueryColumn( String schema, String table, String name, PolySqlType type ) {
        this.schema = schema;
        this.table = table;
        this.name = name;
        this.type = type;
    }


    QueryColumn( String schemaTableName, PolySqlType type ) {
        this( schemaTableName.split( "\\." )[0], schemaTableName.split( "\\." )[1], schemaTableName.split( "\\." )[2], type );
    }


    /**
     * Builds the full name of the column
     *
     * @return full name [schema].[table].[column]
     */
    public String getFullName() {
        return getFullName( schema, table, name );
    }


    public String getFullTableName() {
        return getFullTableName( schema, table );
    }


    /**
     * FullTableName Builder
     *
     * @return fullTableName
     */
    public static String getFullTableName( String schema, String table ) {
        return schema + "." + table;
    }


    public static String getFullName( String schema, String table, String column ) {
        return getFullTableName( schema, table ) + "." + column;
    }


    public static String[] getSplitColumn( String schemaTableColumn ) {
        return schemaTableColumn.split( "\\." );
    }


}
