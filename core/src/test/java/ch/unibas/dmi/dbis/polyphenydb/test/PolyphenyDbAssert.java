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

package ch.unibas.dmi.dbis.polyphenydb.test;


import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;


public class PolyphenyDbAssert {

    public static final boolean ENABLE_SLOW = false;

    static Collection<String> toStringList( ResultSet resultSet, Collection<String> list ) throws SQLException {
        return new ResultSetFormatter().toStringList( resultSet, list );
    }


    /**
     * Converts a {@link ResultSet} to string.
     */
    static class ResultSetFormatter {

        final StringBuilder buf = new StringBuilder();


        public ResultSetFormatter resultSet( ResultSet resultSet ) throws SQLException {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            while ( resultSet.next() ) {
                rowToString( resultSet, metaData );
                buf.append( "\n" );
            }
            return this;
        }


        /**
         * Converts one row to a string.
         */
        ResultSetFormatter rowToString( ResultSet resultSet, ResultSetMetaData metaData ) throws SQLException {
            int n = metaData.getColumnCount();
            if ( n > 0 ) {
                for ( int i = 1; ; i++ ) {
                    buf.append( metaData.getColumnLabel( i ) )
                            .append( "=" )
                            .append( adjustValue( resultSet.getString( i ) ) );
                    if ( i == n ) {
                        break;
                    }
                    buf.append( "; " );
                }
            }
            return this;
        }


        protected String adjustValue( String string ) {
            return string;
        }


        Collection<String> toStringList( ResultSet resultSet, Collection<String> list ) throws SQLException {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            while ( resultSet.next() ) {
                rowToString( resultSet, metaData );
                list.add( buf.toString() );
                buf.setLength( 0 );
            }
            return list;
        }


        /**
         * Flushes the buffer and returns its previous contents.
         */
        public String string() {
            String s = buf.toString();
            buf.setLength( 0 );
            return s;
        }
    }
}
