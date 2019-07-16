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
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager.Collation;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager.Encoding;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager.Pattern;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager.SchemaType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager.TableType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownColumnException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownEncodingException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
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
            transactionHandler.execute( "DROP TABLE IF EXISTS \"default_value\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"table\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"table_privilege\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"schema\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"schema_privilege\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"database\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"database_privilege\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"global_privilege\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"foreign_key\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"index\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"index_columns\";" );

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

            System.out.println( "Default Value:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"default_value\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Global Privilege:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"global_privilege\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Database Privilege:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"database_privilege\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Schema Privilege:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"schema_privilege\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Table Privilege:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"table_privilege\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Column Privilege:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"column_privilege\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Foreign Key:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"foreign_key\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Index:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"index\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Index Columns:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"index_columns\";" );
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

    // -------------------------------------------------------------------------------------------------------------------------------------------------------
    //
    //                                                                     Databases
    //
    // -------------------------------------------------------------------------------------------------------------------------------------------------------


    private static List<CatalogDatabase> databaseFilter( TransactionHandler transactionHandler, String filter ) throws GenericCatalogException, UnknownEncodingException, UnknownCollationException {
        String sql = "SELECT d.\"id\", d.\"name\", u.\"id\", u.\"username\", d.\"encoding\", d.\"collation\", d.\"connection_limit\" FROM \"database\" d, \"user\" u WHERE d.\"owner\" = u.\"id\"" + filter + ";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            List<CatalogDatabase> list = new LinkedList<>();
            while ( rs.next() ) {
                list.add( new CatalogDatabase(
                        rs.getLong( 1 ),
                        rs.getString( 2 ),
                        rs.getInt( 3 ),
                        rs.getString( 4 ),
                        Encoding.getById( rs.getInt( 5 ) ),
                        Collation.getById( rs.getInt( 6 ) ),
                        rs.getInt( 7 )
                ) );
            }
            return list;

        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }

    /**
     * Get all databases which math the specified database name pattern.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param namePattern A pattern for the database name. Set null to get all databases.
     * @return A List of CatalogDatabase
     */
    static List<CatalogDatabase> getDatabases( TransactionHandler transactionHandler, Pattern namePattern ) throws GenericCatalogException, UnknownEncodingException, UnknownCollationException {
        String filter = "";
        if ( namePattern != null ) {
            filter = " AND \"name\" LIKE '" + namePattern.pattern + "'";
        }
        return databaseFilter( transactionHandler, filter );
    }


    /**
     * Get the database with the specified name
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param databaseName The name of the database
     * @return A CatalogDatabase
     */
    static CatalogDatabase getDatabase( TransactionHandler transactionHandler, String databaseName ) throws UnknownEncodingException, UnknownCollationException, UnknownDatabaseException, GenericCatalogException {
        String filter = " AND \"name\" = '" + databaseName + "'";
        List<CatalogDatabase> list = databaseFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownDatabaseException( databaseName );
        }
        return list.get( 0 );
    }


    /**
     * Get the database with the specified database id
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param databaseId The id of the database
     * @return A CatalogDatabase
     */
    static CatalogDatabase getDatabase( TransactionHandler transactionHandler, long databaseId ) throws UnknownEncodingException, UnknownCollationException, UnknownDatabaseException, GenericCatalogException {
        String filter = " AND \"id\" = " + databaseId;
        List<CatalogDatabase> list = databaseFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownDatabaseException( databaseId );
        }
        return list.get( 0 );
    }

    // -------------------------------------------------------------------------------------------------------------------------------------------------------
    //
    //                                                                     Schemas
    //
    // -------------------------------------------------------------------------------------------------------------------------------------------------------


    private static List<CatalogSchema> schemaFilter( TransactionHandler transactionHandler, String filter ) throws UnknownEncodingException, UnknownCollationException, UnknownSchemaTypeException, GenericCatalogException {
        String sql = "SELECT s.\"id\", s.\"name\", d.\"id\", d.\"name\", u.\"id\", u.\"username\", s.\"encoding\", s.\"collation\", s.\"type\" FROM \"schema\" s, \"database\" d, \"user\" u WHERE s.\"database\" = d.\"id\" AND s.\"owner\" = u.\"id\"" + filter + ";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            List<CatalogSchema> list = new LinkedList<>();
            while ( rs.next() ) {
                list.add( new CatalogSchema(
                        rs.getLong( 1 ),
                        rs.getString( 2 ),
                        rs.getLong( 3 ),
                        rs.getString( 4 ),
                        rs.getInt( 5 ),
                        rs.getString( 6 ),
                        Encoding.getById( rs.getInt( 7 ) ),
                        Collation.getById( rs.getInt( 8 ) ),
                        SchemaType.getById( rs.getInt( 9 ) )
                ) );
            }
            return list;
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all schemas in the specified database which match the specified schema name pattern.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param databaseId The id of the database
     * @param schemaNamePattern A pattern for the schema name. Set null to get all schemas.
     * @return A List of CatalogSchemas
     */
    static List<CatalogSchema> getSchemas( TransactionHandler transactionHandler, long databaseId, Pattern schemaNamePattern ) throws GenericCatalogException, UnknownEncodingException, UnknownCollationException, UnknownSchemaTypeException {
        String filter;
        if ( schemaNamePattern != null ) {
            filter = " AND \"database\" = " + databaseId + " AND \"name\" LIKE '" + schemaNamePattern.pattern + "'";
        } else {
            filter = " AND \"database\" = " + databaseId;
        }
        return schemaFilter( transactionHandler, filter );
    }


    /**
     * Get all schemas which match the specified schema name pattern and database name pattern.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param databaseNamePattern A pattern for the database name. Set null to get all schemas.
     * @param schemaNamePattern A pattern for the schema name. Set null to get all schemas.
     * @return A List of CatalogSchemas
     */
    static List<CatalogSchema> getSchemas( TransactionHandler transactionHandler, Pattern databaseNamePattern, Pattern schemaNamePattern ) throws GenericCatalogException, UnknownEncodingException, UnknownCollationException, UnknownSchemaTypeException {
        String filter = "";
        if ( schemaNamePattern != null ) {
            filter += " AND s.\"name\" LIKE '" + schemaNamePattern.pattern + "'";
        }
        if ( databaseNamePattern != null ) {
            filter += " AND d.\"name\" LIKE '" + databaseNamePattern.pattern + "'";
        }
        return schemaFilter( transactionHandler, filter );
    }


    /**
     * Get the schema with the specified id
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param schemaId The id of the schema
     * @return A CatalogSchema
     */
    static CatalogSchema getSchema( TransactionHandler transactionHandler, long schemaId ) throws UnknownEncodingException, UnknownCollationException, GenericCatalogException, UnknownSchemaTypeException, UnknownSchemaException {
        String filter = " AND \"id\" = " + schemaId;
        List<CatalogSchema> list = schemaFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownSchemaException( schemaId );
        }
        return list.get( 0 );
    }


    /**
     * Get the schema with the specified name
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param databaseId The id of the database
     * @param schemaName The name of the schema
     * @return A CatalogSchema
     */
    static CatalogSchema getSchema( TransactionHandler transactionHandler, long databaseId, String schemaName ) throws UnknownEncodingException, UnknownCollationException, GenericCatalogException, UnknownSchemaTypeException, UnknownSchemaException {
        String filter = " AND \"database\" = " + databaseId + " AND \"name\" = '" + schemaName + "'";
        List<CatalogSchema> list = schemaFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownSchemaException( databaseId, schemaName );
        }
        return list.get( 0 );
    }


    /**
     * Get the schema with the specified name
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @return A CatalogSchema
     */
    static CatalogSchema getSchema( TransactionHandler transactionHandler, String databaseName, String schemaName ) throws UnknownEncodingException, UnknownCollationException, GenericCatalogException, UnknownSchemaTypeException, UnknownSchemaException {
        String filter = " AND d.\"name\" = '" + databaseName + "' AND s.\"name\" = '" + schemaName + "'";
        List<CatalogSchema> list = schemaFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownSchemaException( databaseName, schemaName );
        }
        return list.get( 0 );
    }

    // -------------------------------------------------------------------------------------------------------------------------------------------------------
    //
    //                                                                     Tables
    //
    // -------------------------------------------------------------------------------------------------------------------------------------------------------


    private static List<CatalogTable> tableFilter( TransactionHandler transactionHandler, String filter ) throws UnknownEncodingException, UnknownCollationException, UnknownTableTypeException, GenericCatalogException {
        final String sql = "SELECT t.\"id\", t.\"name\", s.\"id\", s.\"name\", d.\"id\", d.\"name\", u.\"id\", u.\"username\", t.\"encoding\", t.\"collation\", t.\"type\", t.\"definition\" FROM \"table\" t, \"schema\" s, \"database\" d, \"user\" u WHERE t.\"schema\" = s.\"id\" AND s.\"database\" = d.\"id\" AND t.\"owner\" = u.\"id\"" + filter;
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            List<CatalogTable> list = new LinkedList<>();
            while ( rs.next() ) {
                list.add( new CatalogTable(
                        rs.getLong( 1 ),
                        rs.getString( 2 ),
                        rs.getLong( 3 ),
                        rs.getString( 4 ),
                        rs.getLong( 5 ),
                        rs.getString( 6 ),
                        rs.getInt( 7 ),
                        rs.getString( 8 ),
                        Encoding.getById( rs.getInt( 9 ) ),
                        Collation.getById( rs.getInt( 10 ) ),
                        TableType.getById( rs.getInt( 11 ) ),
                        rs.getString( 12 )
                ) );
            }
            return list;
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all tables in the specified schema which match the specified table name pattern.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param schemaId The id of the schema
     * @param tableNamePattern A pattern for the table name. Set null to get all tables.
     * @return A List of CatalogTables
     */
    static List<CatalogTable> getTables( TransactionHandler transactionHandler, long schemaId, Pattern tableNamePattern ) throws GenericCatalogException, UnknownEncodingException, UnknownCollationException, UnknownTableTypeException {
        String filter;
        if ( tableNamePattern != null ) {
            filter = " AND \"schema\" = " + schemaId + " AND \"name\" LIKE '" + tableNamePattern.pattern + "';";
        } else {
            filter = " AND \"schema\" = " + schemaId + ";";
        }
        return tableFilter( transactionHandler, filter );
    }


    /**
     * Get all tables of the specified database which match the specified table name pattern and schema name pattern.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param databaseId The id of the database
     * @param schemaNamePattern A pattern for the schema name. Set null to get all.
     * @param tableNamePattern A pattern for the table name. Set null to get all.
     * @return A List of CatalogTables
     */
    static List<CatalogTable> getTables( TransactionHandler transactionHandler, long databaseId, Pattern schemaNamePattern, Pattern tableNamePattern ) throws GenericCatalogException, UnknownEncodingException, UnknownCollationException, UnknownTableTypeException {
        String filter = " AND s.\"database\" = " + databaseId;
        if ( tableNamePattern != null ) {
            filter += " AND t.\"name\" LIKE '" + tableNamePattern.pattern + "'";
        }
        if ( schemaNamePattern != null ) {
            filter += " AND s.\"name\" LIKE '" + schemaNamePattern.pattern + "'";
        }
        filter += ";";
        return tableFilter( transactionHandler, filter );
    }


    /**
     * Get all tables which match the specified table name pattern and schema name pattern.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param databaseNamePattern A pattern for the database name. Set null to get all.
     * @param schemaNamePattern A pattern for the schema name. Set null to get all.
     * @param tableNamePattern A pattern for the table name. Set null to get all.
     * @return A List of CatalogTables
     */
    static List<CatalogTable> getTables( TransactionHandler transactionHandler, Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern ) throws GenericCatalogException, UnknownEncodingException, UnknownCollationException, UnknownTableTypeException {
        String filter = "";
        if ( tableNamePattern != null ) {
            filter += " AND t.\"name\" LIKE '" + tableNamePattern.pattern + "'";
        }
        if ( schemaNamePattern != null ) {
            filter += " AND s.\"name\" LIKE '" + schemaNamePattern.pattern + "'";
        }
        if ( databaseNamePattern != null ) {
            filter += " AND d.\"name\" LIKE '" + databaseNamePattern.pattern + "'";
        }
        filter += ";";
        return tableFilter( transactionHandler, filter );
    }


    /**
     * Get the specified table
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param tableId The id of the table
     * @return A CatalogTable
     */
    static CatalogTable getTable( TransactionHandler transactionHandler, long tableId ) throws UnknownEncodingException, UnknownCollationException, GenericCatalogException, UnknownTableTypeException, UnknownTableException {
        String filter = " AND \"id\" = " + tableId + ";";
        List<CatalogTable> list = tableFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownTableException( tableId );
        }
        return list.get( 0 );
    }


    /**
     * Get the table with the specified name in the specified schema
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param schemaId The id of the schema
     * @param tableName The name of the table
     * @return A CatalogTable
     */
    static CatalogTable getTable( TransactionHandler transactionHandler, long schemaId, String tableName ) throws UnknownEncodingException, UnknownCollationException, GenericCatalogException, UnknownTableTypeException, UnknownTableException {
        String filter = " AND \"schema\" = " + schemaId + " AND \"name\" = '" + tableName + "';";
        List<CatalogTable> list = tableFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownTableException( schemaId, tableName );
        }
        return list.get( 0 );
    }


    /**
     * Get the table with the specified name in the specified schema of the specified database
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param databaseId The id of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @return A CatalogTable
     */
    static CatalogTable getTable( TransactionHandler transactionHandler, long databaseId, String schemaName, String tableName ) throws UnknownEncodingException, UnknownCollationException, GenericCatalogException, UnknownTableTypeException, UnknownTableException {
        String filter = " AND s.\"database\" = " + databaseId + " AND s.\"name\" = '" + schemaName + "' AND t.\"name\" = '" + tableName + "';";
        List<CatalogTable> list = tableFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownTableException( databaseId, schemaName, tableName );
        }
        return list.get( 0 );
    }


    /**
     * Get the table with the specified name in the specified schema of the specified database
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @return A CatalogTable
     */
    static CatalogTable getTable( TransactionHandler transactionHandler, String databaseName, String schemaName, String tableName ) throws UnknownEncodingException, UnknownCollationException, GenericCatalogException, UnknownTableTypeException, UnknownTableException {
        String filter = " AND d.\"name\" = '" + databaseName + "' AND s.\"name\" = '" + schemaName + "' AND t.\"name\" = '" + tableName + "';";
        List<CatalogTable> list = tableFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownTableException( databaseName, schemaName, tableName );
        }
        return list.get( 0 );
    }

    // -------------------------------------------------------------------------------------------------------------------------------------------------------
    //
    //                                                                     Columns
    //
    // -------------------------------------------------------------------------------------------------------------------------------------------------------


    private static List<CatalogColumn> columnFilter( TransactionHandler transactionHandler, String filter ) throws UnknownEncodingException, UnknownCollationException, GenericCatalogException, UnknownTypeException {
        String sql = "SELECT c.\"id\", c.\"name\", t.\"id\", t.\"name\", s.\"id\", s.\"name\", d.\"id\", d.\"name\", c.\"position\", c.\"type\", c.\"length\", c.\"precision\", c.\"nullable\", c.\"encoding\", c.\"collation\", c.\"force_default\" FROM \"column\" c, \"table\" t, \"schema\" s, \"database\" d WHERE c.\"table\" = t.\"id\" AND t.\"schema\" = s.\"id\"  AND s.\"database\" = d.\"id\"" + filter + " ORDER BY d.\"id\", s.\"id\", t.\"id\", c.\"position\";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            List<CatalogColumn> list = new LinkedList<>();
            while ( rs.next() ) {
                list.add( new CatalogColumn(
                        rs.getLong( 1 ),
                        rs.getString( 2 ),
                        rs.getLong( 3 ),
                        rs.getString( 4 ),
                        rs.getLong( 5 ),
                        rs.getString( 6 ),
                        rs.getLong( 7 ),
                        rs.getString( 8 ),
                        rs.getInt( 9 ),
                        PolySqlType.getByTypeCode( rs.getInt( 10 ) ),
                        rs.getInt( 11 ),
                        rs.getInt( 12 ),
                        rs.getBoolean( 13 ),
                        Encoding.getById( rs.getInt( 14 ) ),
                        Collation.getById( rs.getInt( 15 ) ),
                        rs.getBoolean( 16 )
                ) );
            }
            return list;
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns all columns of the specified table.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param tableId The id of the table
     * @return A CatalogColumn
     */
    static List<CatalogColumn> getColumns( TransactionHandler transactionHandler, long tableId ) throws UnknownEncodingException, UnknownCollationException, GenericCatalogException, UnknownTypeException {
        String filter = " AND c.\"table\" = " + tableId;
        return columnFilter( transactionHandler, filter );
    }


    /**
     * Returns all columns of the specified table in the specified schema of the specified database.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param databaseNamePattern Pattern for the database name
     * @param schemaNamePattern Pattern for the schema name
     * @param tableNamePattern Pattern for the table name
     * @param columnNamePattern Pattern for the column name
     * @return A CatalogColumn
     */
    static List<CatalogColumn> getColumns( TransactionHandler transactionHandler, Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern, Pattern columnNamePattern ) throws UnknownEncodingException, UnknownCollationException, GenericCatalogException, UnknownTypeException {
        String filter = "";
        if ( columnNamePattern != null ) {
            filter += " AND c.\"name\" LIKE '" + columnNamePattern.pattern + "'";
        }
        if ( tableNamePattern != null ) {
            filter += " AND t.\"name\" LIKE '" + tableNamePattern.pattern + "'";
        }
        if ( schemaNamePattern != null ) {
            filter += " AND s.\"name\" LIKE '" + schemaNamePattern.pattern + "'";
        }
        if ( schemaNamePattern != null ) {
            filter += " AND d.\"name\" LIKE '" + databaseNamePattern.pattern + "'";
        }
        return columnFilter( transactionHandler, filter );
    }


    /**
     * Returns the column with the specified table.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param tableId The id of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     */
    static CatalogColumn getColumn( TransactionHandler transactionHandler, long tableId, String columnName ) throws UnknownEncodingException, UnknownCollationException, GenericCatalogException, UnknownTypeException, UnknownColumnException {
        String filter = " AND c.\"table\" = " + tableId + " AND c.\"name\" = '" + columnName + "';";
        List<CatalogColumn> list = columnFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownColumnException( tableId, columnName );
        }
        return list.get( 0 );
    }


    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     */
    static CatalogColumn getColumn( TransactionHandler transactionHandler, String databaseName, String schemaName, String tableName, String columnName ) throws UnknownEncodingException, UnknownCollationException, GenericCatalogException, UnknownTypeException, UnknownColumnException {
        String filter = " AND d.\"name\" = '" + databaseName + "' AND s.\"name\" = '" + schemaName + "' AND t.\"name\" = '" + tableName + "' AND c.\"name\" = '" + columnName + "';";
        List<CatalogColumn> list = columnFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownColumnException( databaseName, schemaName, tableName, columnName );
        }
        return list.get( 0 );
    }

    // -------------------------------------------------------------------------------------------------------------------------------------------------------
    //
    //                                                                     User
    //
    // -------------------------------------------------------------------------------------------------------------------------------------------------------


    private static List<CatalogUser> userFilter( TransactionHandler transactionHandler, String filter ) throws GenericCatalogException {
        String sql = "SELECT \"id\", \"username\", \"password\" FROM \"user\"";
        if ( filter.length() > 0 ) {
            sql += " WHERE " + filter;
        }
        sql += ";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            List<CatalogUser> list = new LinkedList<>();
            while ( rs.next() ) {
                list.add( new CatalogUser(
                        rs.getInt( 1 ),
                        rs.getString( 2 ),
                        rs.getString( 3 )
                ) );
            }
            return list;
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the user with the specified name.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param id The id of the user
     * @return A CatalogColumn
     */
    static CatalogUser getUser( TransactionHandler transactionHandler, int id ) throws GenericCatalogException, UnknownUserException {
        String filter = " \"id\" = " + id;
        List<CatalogUser> list = userFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownUserException( id );
        }
        return list.get( 0 );
    }


    /**
     * Returns the user with the specified name.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param username The username of the user
     * @return A CatalogColumn
     */
    static CatalogUser getUser( TransactionHandler transactionHandler, String username ) throws GenericCatalogException, UnknownUserException {
        String filter = " \"username\" = '" + username + "'";
        List<CatalogUser> list = userFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownUserException( username );
        }
        return list.get( 0 );
    }

}
