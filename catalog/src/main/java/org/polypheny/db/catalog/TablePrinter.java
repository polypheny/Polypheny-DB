/*
 * Copyright 2019-2020 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.catalog;


import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import lombok.extern.slf4j.Slf4j;


/**
 * Utility to which returns rows from a given ResultSet as String, formatted to look like a table with rows and columns with borders.
 */
@Slf4j
public class TablePrinter {

    public static final int DEFAULT_MAX_ROWS = 25;
    public static final int DEFAULT_MAX_DATA_LENGTH = 25;
    private static final String CROP_STRING = "[...]";
    private static final String NULL_STRING = "NULL";
    private static final String BINARY_STRING = "BINARY";


    public static String processResultSet( ResultSet rs ) {
        return processResultSet( rs, DEFAULT_MAX_ROWS, DEFAULT_MAX_DATA_LENGTH );
    }


    public static String processResultSet( ResultSet rs, int maxRows, int maxLength ) {
        StringBuilder sb = new StringBuilder();

        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int totalColumns = rsmd.getColumnCount();
            Column[] columns = new Column[totalColumns];

            // get labels
            for ( int i = 0; i < totalColumns; i++ ) {
                columns[i] = new Column();
                columns[i].addData( rsmd.getColumnLabel( i + 1 ), maxLength );
            }

            // get Data
            int totalPrintRows = 0;
            int totalRows = 0;
            while ( rs.next() ) {
                totalRows++;
                if ( totalPrintRows < maxRows ) {
                    totalPrintRows++;
                    for ( int columnIndex = 0; columnIndex < totalColumns; columnIndex++ ) {
                        final String stringValue = rs.getString( columnIndex + 1 );

                        switch ( rsmd.getColumnType( columnIndex + 1 ) ) {
                            case Types.BINARY:
                            case Types.VARBINARY:
                            case Types.LONGVARBINARY:
                            case Types.BLOB:
                                columns[columnIndex].addData( BINARY_STRING, maxLength );
                                break;

                            default:
                                columns[columnIndex].addData( stringValue == null ? NULL_STRING : stringValue, maxLength );
                                break;
                        }
                    }
                }
            }

            // build table
            String horizontalLine = getHorizontalLine( columns );
            for ( int rowIndex = 0; rowIndex <= totalPrintRows; rowIndex++ ) {
                sb.append( horizontalLine );
                for ( int columnIndex = 0; columnIndex < totalColumns; columnIndex++ ) {
                    String line = columns[columnIndex].getData( rowIndex );
                    sb.append( String.format( "| %" + columns[columnIndex].maxLength + "s ", line ) );
                }
                sb.append( "|\n" );
            }
            sb.append( horizontalLine );
            sb.append( "Printed " ).append( totalPrintRows ).append( " rows out of " ).append( totalRows ).append( " rows\n" );
        } catch ( SQLException e ) {
            if ( log.isErrorEnabled() ) {
                log.error( "SQLException while processing the ResultSet object.", e );
            }
        }

        return sb.toString();
    }


    private static String getHorizontalLine( Column[] columns ) {
        StringBuilder sb = new StringBuilder();

        for ( Column column : columns ) {
            sb.append( "+" );
            for ( int j = 0; j < column.maxLength + 2; j++ ) {
                sb.append( "-" );
            }
        }
        sb.append( "+\n" );

        return sb.toString();
    }


    private static class Column {

        private int maxLength = 0;
        private final ArrayList<String> data = new ArrayList<>();


        void addData( String dataStr, int maxLength ) {
            if ( dataStr.length() > maxLength ) {
                dataStr = dataStr.substring( 0, maxLength );
                dataStr += CROP_STRING;
            }
            if ( this.maxLength < dataStr.length() ) {
                this.maxLength = dataStr.length();
            }
            data.add( dataStr );
        }


        private String getData( int row ) {
            return data.get( row );
        }
    }
}
