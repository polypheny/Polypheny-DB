/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

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
        this.schema = schema.replace( "\\", "" ).replace( "\"", "" );
        this.table = table.replace( "\\", "" ).replace( "\"", "" );
        this.name = name.replace( "\\", "" ).replace( "\"", "" );
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
    public static String getQualifiedTableName( String schema, String table ) {
        return schema + "\".\"" + table;
    }


    public static String getQualifiedColumnName( String schema, String table, String column ) {
        return "\"" + getQualifiedTableName( schema, table ) + "\".\"" + column;
    }


    public static String[] getSplitColumn( String schemaTableColumn ) {
        return schemaTableColumn.split( "\\." );
    }

}
