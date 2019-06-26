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


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.UnknownTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.Collation;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.Encoding;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.TableType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownEncodingException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableTypeException;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class provides methods to interact with the catalog database. All SQL-stuff should be in this class.
 */
final class Statements {

    private static final Logger logger = LoggerFactory.getLogger( Statements.class );


    /**
     * Empty private constructor to prevent instantiation.
     */
    private Statements() {
        // empty
    }


    /**
     * Create the catalog database schema.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     */
    static void createSchema( TransactionHandler transactionHandler ) {
        logger.debug( "Creating the catalog schema" );
        boolean result = ScriptRunner.runScript( new InputStreamReader( Statements.class.getResourceAsStream( "/catalogSchema.sql" ) ), transactionHandler, false );
        if ( !result ) {
            logger.error( "Exception while creating catalog schema" );
        }
    }


    /**
     * Drop the catalog schema.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     */
    static void dropSchema( TransactionHandler transactionHandler ) {
        logger.debug( "Dropping the catalog schema" );
        try {
            transactionHandler.execute( "DROP TABLE IF EXISTS \"column\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"column_privilege\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"database\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"database_privilege\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"global_privilege\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"schema\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"schema_privilege\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"table\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"table_privilege\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"user\";" );
        } catch ( SQLException e ) {
            logger.error( "Exception while dropping catalog schema", e );
        }
    }


    /**
     * Print the current content of the catalog tables to stdout. Used for debugging purposes only!
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     */
    static void print( TransactionHandler transactionHandler ) {
        try {
            System.out.println( "User:" );
            ResultSet resultSet = transactionHandler.executeSelect( "SELECT * FROM \"user\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Database:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"database\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Schema:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"schema\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Table:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"table\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Column:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"column\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );
        } catch ( SQLException e ) {
            e.printStackTrace();
        }
    }


    /**
     * Export the current schema of Polypheny-DB as bunch of PolySQL statements. Currently printed to stdout for debugging purposes.
     *
     * !!! This method is only partially implemented !!!
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     */
    static void export( TransactionHandler transactionHandler ) {
        try {
            ResultSet resultSet = transactionHandler.executeSelect( "SELECT * FROM \"table\";" );
            while ( resultSet.next() ) {
                String internal_name = resultSet.getString( "internal_name" );
                String schema = resultSet.getString( "schema" );
                String name = resultSet.getString( "name" );
                TableType type = TableType.getById( resultSet.getInt( "type" ) );
                if ( type == TableType.TABLE ) {
                    System.out.println( "CREATE TABLE \"" + name + "\" (" );
                    ResultSet columnResultSet = transactionHandler.executeSelect( "SELECT * FROM \"column\" WHERE \"table\" = '" + internal_name + "' ORDER BY \"position\";" );
                    while ( columnResultSet.next() ) {
                        String columnName = columnResultSet.getString( "name" );
                        PolySqlType columnType = PolySqlType.getByTypeCode( columnResultSet.getInt( "type" ) );
                        long length = columnResultSet.getLong( "length" );
                        long precision = columnResultSet.getLong( "precision" );
                        boolean nullable = columnResultSet.getBoolean( "nullable" );
                        Encoding encoding = Encoding.getById( columnResultSet.getInt( "encoding" ) );
                        Collation collation = Collation.getById( columnResultSet.getInt( "collation" ) );
                        java.io.Serializable defaultValue = columnResultSet.getString( "default_value" );
                        int autoIncrementStartValue = columnResultSet.getInt( "autoIncrement_start_value" );
                        boolean forceDefault = columnResultSet.getBoolean( "force_default" );

                        System.out.print( "    " + columnName + " " + columnType.name() );

                        // type arguments (length and precision)
                        if ( length != 0 ) {
                            System.out.print( "(" + length );
                            if ( precision != 0 ) {
                                System.out.print( ", " + precision );
                            }
                            System.out.print( ")" );
                        }
                        System.out.print( " " );

                        // Nullability
                        if ( nullable ) {
                            System.out.print( "NULL " );
                        } else {
                            System.out.print( "NOT NULL " );
                        }

                        // default value
                        if ( defaultValue != null ) {
                            System.out.print( "DEFAULT \"" + defaultValue.toString() + "\" " );
                        } else if ( autoIncrementStartValue > 0 ) {
                            System.out.print( "DEFAULT AUTO INCREMENT STARTING WITH " + autoIncrementStartValue + " " );
                        }

                        if ( forceDefault ) {
                            System.out.print( "FORCE " );
                        }

                        // encoding and collation
                        System.out.print( "ENCODING " + encoding.name() + " COLLATION " + collation.name() );

                        System.out.println( "," );
                    }
                    System.out.println( ");" );
                }
            }
        } catch ( SQLException | UnknownTableTypeException | UnknownTypeException | UnknownEncodingException | UnknownCollationException e ) {
            e.printStackTrace();
        }
    }


    /**
     * Get all tables of a database which meet the specified filter criteria
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @return A List of CatalogTables
     */
    static List<CatalogTable> getAllTables( TransactionHandler transactionHandler ) throws SQLException, UnknownEncodingException, UnknownTableTypeException, UnknownCollationException {
        String sql = "SELECT \"internal_name\", \"name\", \"owner\", \"encoding\", \"collation\", \"type\", \"definition\", \"chunk_column\", \"chunk_size\" FROM \"table\";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            List<CatalogTable> list = new LinkedList<>();
            while ( rs.next() ) {
                /*InternalName chunkColumn = null;
                if ( rs.getString( 8 ) != null && rs.getString( 8 ).length() == 10 ) {
                    chunkColumn = new InternalName( rs.getString( 8 ) );
                }
                Integer chunkSize = null;
                if ( rs.getObject( 9 ) instanceof Integer ) {
                    chunkSize = (Integer) rs.getObject( 9 );
                }
                /*list.add( new CatalogTable(
                        new InternalName( rs.getString( 1 ) ),
                        rs.getString( 2 ),
                        rs.getInt( 3 ),
                        Encoding.getById( rs.getInt( 4 ) ),
                        Collation.getById( rs.getInt( 5 ) ),
                        TableType.getById( rs.getInt( 6 ) ),
                        rs.getString( 7 ),
                        chunkColumn,
                        chunkSize
                ) );
                */
            }
            return list;

        }
    }


}
