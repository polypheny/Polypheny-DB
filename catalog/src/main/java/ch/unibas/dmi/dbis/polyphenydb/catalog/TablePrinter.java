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

package ch.unibas.dmi.dbis.polyphenydb.catalog;


import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility to which returns rows from a given ResultSet as String, formatted to look like a table with rows and columns with borders.
 */
public class TablePrinter {

    private static final Logger LOGGER = LoggerFactory.getLogger( TablePrinter.class );

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
            sb.append( "Printed " + totalPrintRows + " rows out of " + totalRows + " rows\n" );
        } catch ( SQLException e ) {
            if ( LOGGER.isErrorEnabled() ) {
                LOGGER.error( "SQLException while processing the ResultSet object.", e );
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
        private ArrayList<String> data = new ArrayList<>();


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
