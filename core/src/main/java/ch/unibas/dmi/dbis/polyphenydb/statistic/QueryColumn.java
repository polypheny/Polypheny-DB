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
        this.schema = schema.replace("\\", "").replace( "\"", "" );
        this.table = table.replace("\\", "").replace( "\"", "" );
        this.name = name.replace("\\", "").replace( "\"", "" );
        this.type = type;
    }


    QueryColumn( String schemaTableName, PolySqlType type ) {
        this( schemaTableName.split( "\\." )[0], schemaTableName.split( "\\." )[1], schemaTableName.split( "\\." )[2], type );
    }


    /**
     * Builds the qualified name of the column
     *
     * @return qualifiedColumnName
     */
    public String getQualifiedColumnName() {
        return getQualifiedColumnName( schema, table, name );
    }


    public String getQualifiedTableName() {
        return getQualifiedTableName( schema, table );
    }


    /**
     * QualifiedTableName Builder
     *
     * @return fullTableName
     */
    public static String getQualifiedTableName(String schema, String table ) {
        return schema + "\".\"" + table;
    }


    public static String getQualifiedColumnName(String schema, String table, String column ) {
        return "\"" + getQualifiedTableName( schema, table ) + "\".\"" + column;
    }


    public static String[] getSplitColumn( String schemaTableColumn ) {
        return schemaTableColumn.split( "\\." );
    }


}
