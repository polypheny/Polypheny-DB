/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
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

package ch.unibas.dmi.dbis.polyphenydb.adapter.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelProtoDataType;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema.TableType;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import javax.sql.DataSource;
import org.apache.calcite.avatica.SqlType;


/**
 * Helper class for determining the schema for a JDBC adapter without a catalog.
 */
public class JdbcTableComputer {


    public static ImmutableMap<String, JdbcTable> computeTables( String database, String schema, DataSource dataSource ) {
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            if ( metaData.getJDBCMajorVersion() > 4 || (metaData.getJDBCMajorVersion() == 4 && metaData.getJDBCMinorVersion() >= 1) ) {
                // From JDBC 4.1, catalog and schema can be retrieved from the connection object, hence try to get it from there if it was not specified by user
                database = Util.first( database, connection.getCatalog() );
                schema = Util.first( schema, connection.getSchema() );
            } else {
                database = database;
                schema = schema;
            }
            resultSet = metaData.getTables( database, schema, null, null );
            final ImmutableMap.Builder<String, JdbcTable> builder = ImmutableMap.builder();
            while ( resultSet.next() ) {
                final String tableName = resultSet.getString( 3 );
                final String catalogName = resultSet.getString( 1 );
                final String schemaName = resultSet.getString( 2 );
                final String tableTypeName = resultSet.getString( 4 );
                // Clean up table type. In particular, this ensures that 'SYSTEM TABLE', returned by Phoenix among others, maps to TableType.SYSTEM_TABLE.
                // We know enum constants are upper-case without spaces, so we can't make things worse.
                //
                // PostgreSQL returns tableTypeName==null for pg_toast* tables. This can happen if you start JdbcSchema off a "public" PG schema.
                // The tables are not designed to be queried by users, however we do not filter them as we keep all the other table types.
                final String tableTypeName2 =
                        tableTypeName == null
                                ? null
                                : tableTypeName.toUpperCase( Locale.ROOT ).replace( ' ', '_' );
                final TableType tableType = Util.enumVal( TableType.OTHER, tableTypeName2 );
                if ( tableType == TableType.OTHER && tableTypeName2 != null ) {
                    System.out.println( "Unknown table type: " + tableTypeName2 );
                }
                final JdbcTable table = new JdbcTable( null, catalogName, schemaName, tableName, tableType, getRelDataType( dataSource, catalogName, schemaName, tableName ) );
                builder.put( tableName, table );
            }
            return builder.build();
        } catch ( SQLException e ) {
            throw new RuntimeException( "Exception while reading tables", e );
        } finally {
            close( connection, null, resultSet );
        }
    }


    private static RelProtoDataType getRelDataType( DataSource dataSource, String catalogName, String schemaName, String tableName ) throws SQLException {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            return getRelDataType( metaData, catalogName, schemaName, tableName );
        } finally {
            close( connection, null, null );
        }
    }


    private static RelProtoDataType getRelDataType( DatabaseMetaData metaData, String catalogName, String schemaName, String tableName ) throws SQLException {
        final ResultSet resultSet = metaData.getColumns( catalogName, schemaName, tableName, null );

        // Temporary type factory, just for the duration of this method. Allowable because we're creating a proto-type, not a type; before being used, the proto-type will be copied into a real type factory.
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
        final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
        while ( resultSet.next() ) {
            final String columnName = resultSet.getString( 4 );
            final int dataType = resultSet.getInt( 5 );
            final String typeString = resultSet.getString( 6 );
            final int precision;
            final int scale;
            switch ( SqlType.valueOf( dataType ) ) {
                case TIMESTAMP:
                case TIME:
                    precision = resultSet.getInt( 9 ); // SCALE
                    scale = 0;
                    break;
                default:
                    precision = resultSet.getInt( 7 ); // SIZE
                    scale = resultSet.getInt( 9 ); // SCALE
                    break;
            }
            RelDataType sqlType = sqlType( typeFactory, dataType, precision, scale, typeString );
            boolean nullable = resultSet.getInt( 11 ) != DatabaseMetaData.columnNoNulls;
            fieldInfo.add( columnName, sqlType ).nullable( nullable );
        }
        resultSet.close();
        return RelDataTypeImpl.proto( fieldInfo.build() );
    }


    private static RelDataType sqlType( RelDataTypeFactory typeFactory, int dataType, int precision, int scale, String typeString ) {
        // Fall back to ANY if type is unknown
        final SqlTypeName sqlTypeName = Util.first( SqlTypeName.getNameForJdbcType( dataType ), SqlTypeName.ANY );
        switch ( sqlTypeName ) {
            case ARRAY:
                RelDataType component = null;
                if ( typeString != null && typeString.endsWith( " ARRAY" ) ) {
                    // E.g. hsqldb gives "INTEGER ARRAY", so we deduce the component type "INTEGER".
                    final String remaining = typeString.substring( 0, typeString.length() - " ARRAY".length() );
                    component = parseTypeString( typeFactory, remaining );
                }
                if ( component == null ) {
                    component = typeFactory.createTypeWithNullability( typeFactory.createSqlType( SqlTypeName.ANY ), true );
                }
                return typeFactory.createArrayType( component, -1 );
        }
        if ( precision >= 0 && scale >= 0 && sqlTypeName.allowsPrecScale( true, true ) ) {
            return typeFactory.createSqlType( sqlTypeName, precision, scale );
        } else if ( precision >= 0 && sqlTypeName.allowsPrecNoScale() ) {
            return typeFactory.createSqlType( sqlTypeName, precision );
        } else {
            assert sqlTypeName.allowsNoPrecNoScale();
            return typeFactory.createSqlType( sqlTypeName );
        }
    }


    /**
     * Given "INTEGER", returns BasicSqlType(INTEGER).
     * Given "VARCHAR(10)", returns BasicSqlType(VARCHAR, 10).
     * Given "NUMERIC(10, 2)", returns BasicSqlType(NUMERIC, 10, 2).
     */
    private static RelDataType parseTypeString( RelDataTypeFactory typeFactory, String typeString ) {
        int precision = -1;
        int scale = -1;
        int open = typeString.indexOf( "(" );
        if ( open >= 0 ) {
            int close = typeString.indexOf( ")", open );
            if ( close >= 0 ) {
                String rest = typeString.substring( open + 1, close );
                typeString = typeString.substring( 0, open );
                int comma = rest.indexOf( "," );
                if ( comma >= 0 ) {
                    precision = Integer.parseInt( rest.substring( 0, comma ) );
                    scale = Integer.parseInt( rest.substring( comma ) );
                } else {
                    precision = Integer.parseInt( rest );
                }
            }
        }
        try {
            final SqlTypeName typeName = SqlTypeName.valueOf( typeString );
            return typeName.allowsPrecScale( true, true )
                    ? typeFactory.createSqlType( typeName, precision, scale )
                    : typeName.allowsPrecScale( true, false )
                            ? typeFactory.createSqlType( typeName, precision )
                            : typeFactory.createSqlType( typeName );
        } catch ( IllegalArgumentException e ) {
            return typeFactory.createTypeWithNullability( typeFactory.createSqlType( SqlTypeName.ANY ), true );
        }
    }


    private static void close( Connection connection, Statement statement, ResultSet resultSet ) {
        if ( resultSet != null ) {
            try {
                resultSet.close();
            } catch ( SQLException e ) {
                // ignore
            }
        }
        if ( statement != null ) {
            try {
                statement.close();
            } catch ( SQLException e ) {
                // ignore
            }
        }
        if ( connection != null ) {
            try {
                connection.close();
            } catch ( SQLException e ) {
                // ignore
            }
        }
    }

}
