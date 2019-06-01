/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2017 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
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
 */

package ch.unibas.dmi.dbis.polyphenydb.scu.catalog;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.UnknownTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.Collation;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.Encoding;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.TableType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.InternalName;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.ExceedsMaximumNumberOfColumnsException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.ExceedsMaximumNumberOfDatabasesException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.ExceedsMaximumNumberOfSchemasException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.ExceedsMaximumNumberOfTablesException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownColumnException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownEncodingException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import org.hsqldb.jdbc.pool.JDBCXID;
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
        boolean result = ScriptRunner.runScript( new InputStreamReader( Statements.class.getClass().getResourceAsStream( "/catalogSchema.sql" ) ), transactionHandler, false );
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
     * Adds a database record to the catalog
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param internalName The unique internal name of the database.
     * @param databaseName The name for the database.
     * @param ownerId The id of the owner of the database (the user which has created the database).
     * @param encoding The default encoding for the database. This is used if there is no encoding specified for a column.
     * @param collation The default collation for the database. This is used if there is no collation specified for a column.
     * @param connectionLimit The maximum number of concurrent connections to a database.
     * @return A boolean indicating if the database record was successfully created.
     */
    static boolean addDatabase( TransactionHandler transactionHandler, InternalName internalName, String databaseName, int ownerId, Encoding encoding, Collation collation, int connectionLimit ) throws SQLException {
        // insert database record
        String insertSql = "INSERT INTO \"database\"(\"internal_name\", \"internal_name_part\", \"name\", \"owner\", \"encoding\", \"collation\", \"connection_limit\") VALUES('" + internalName + "', " + internalName.getDatabasePart() + ", '" + databaseName + "', " + ownerId + ", " + encoding.getId() + ", " + collation.getId() + ", " + connectionLimit + ")";
        return transactionHandler.executeUpdate( insertSql ) == 1;
    }


    /**
     * Adds a schema to the catalog
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param internalName The unique internal name of the schema.
     * @param schemaName The name of the schema.
     * @param ownerId The id of the owner of the schema (the user which has created it).
     * @param encoding The default encoding for the schema. This is used if there is no encoding specified for a column.
     * @param collation The default collation for the schema. This is used if there is no collation specified for a column.
     * @return A boolean indicating if the schema record was successfully created.
     */
    static boolean addSchema( TransactionHandler transactionHandler, InternalName internalName, String schemaName, int ownerId, Encoding encoding, Collation collation ) throws SQLException {
        // insert database record
        String insertSql = "INSERT INTO \"schema\"(\"internal_name\", \"internal_name_part\", \"database\", \"name\", \"owner\", \"encoding\", \"collation\") VALUES('" + internalName + "', " + internalName.getSchemaPart() + ", '" + internalName.getDatabase() + "', '" + schemaName + "', " + ownerId + ", " + encoding.getId() + ", " + collation.getId() + ")";
        return transactionHandler.executeUpdate( insertSql ) == 1;
    }


    /**
     * Adds a table to the catalog
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param internalName The unique internal name of the table.
     * @param tableName The name of the table.
     * @param ownerId The id of the owner of the table (the user which has created it).
     * @param encoding The default encoding for the table. This is used if there is no encoding specified for a column.
     * @param collation The default collation for the table. This is used if there is no collation specified for a column.
     * @param type The type of the table (e.g., table or view)
     * @param definition A definition for that table (empty for tables of type table)
     * @return A boolean indicating if the table record was successfully created.
     */
    static boolean addTable( TransactionHandler transactionHandler, InternalName internalName, String tableName, int ownerId, Encoding encoding, Collation collation, TableType type, String definition ) throws SQLException {
        // insert table record
        String insertSql = "INSERT INTO \"table\"(\"internal_name\", \"internal_name_part\", \"schema\", \"name\", \"owner\", \"encoding\", \"collation\", \"type\", \"definition\") VALUES('" + internalName + "', " + internalName.getTablePart() + ", '" + internalName.getSchema() + "', '" + tableName + "', " + ownerId + ", " + encoding.getId() + ", " + collation.getId() + ", " + type.getId() + ", ";
        if ( definition == null ) {
            insertSql += "null";
        } else {
            insertSql += "'" + definition + "'";
        }
        insertSql += ")";

        return transactionHandler.executeUpdate( insertSql ) == 1;
    }


    /**
     * Adds a column record to the catalog
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param internalName The unique internal name of the column.
     * @param columnName The name of the column.
     * @param type The data type of the column.
     * @param position The position of the column in the table.
     * @param nullable Specify if this column can contain null.
     * @param defaultValue A serialized java object which can be used to generate the default value
     * @param forceDefault Specifies if the default value (or auto increment) should be enforced, which means that it is not possible to set a value for this column if set to force.
     * @param autoincrementStartValue The start value for auto increment. If set, auto increment is enabled. Can be null.
     * @param autoIncrementNextValue The next value for this column if using auto increment. Can be null.
     * @param encoding The encoding for this column.
     * @param collation The collation for the column.
     * @param length The length of this column. Set if required by the data type (e.g., varchar), null if not.
     * @param precision The precision of this column. Set if required by the data type (e.g., decimal), null if not.
     * @return A boolean indicating if the column record was successfully created.
     */
    static boolean addColumn( TransactionHandler transactionHandler, InternalName internalName, String columnName, PolySqlType type, int position, boolean nullable, java.io.Serializable defaultValue, boolean forceDefault, Long autoincrementStartValue, Long autoIncrementNextValue, Encoding encoding, Collation collation, Integer length, Integer precision ) throws SQLException {
        // insert column record
        String insertSql =
                "INSERT INTO \"column\"(\"internal_name\", \"internal_name_part\", \"table\", \"name\", \"position\", \"type\", \"length\", \"precision\", \"nullable\", \"encoding\", \"collation\", \"autoincrement_start_value\", \"autoincrement_next_value\", \"force_default\", \"default_value\") VALUES('" + internalName + "', " + internalName.getColumnPart() + ", '" + internalName.getTable() + "', '" + columnName + "', " + position + ", " + type.getTypeCode() + ", " + length + ", " + precision
                        + ", " + nullable + ", " + encoding.getId() + ", " + collation.getId() + ", " + autoincrementStartValue + ", " + autoIncrementNextValue + ", " + forceDefault + ", ";
        if ( defaultValue == null ) {
            insertSql += "null";
        } else {
            insertSql += "'" + defaultValue + "'";
        }
        insertSql += ")";
        if ( logger.isDebugEnabled() ) {
            logger.debug( insertSql );
        }
        return transactionHandler.executeUpdate( insertSql ) == 1;
    }


    /**
     * Deletes a table record from the catalog.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param table The InternalName of the table to delete.
     * @return A boolean indicating if the table record was successfully deleted.
     */
    public static boolean deleteTable( XATransactionHandler transactionHandler, InternalName table ) throws SQLException {
        String sql = "DELETE FROM \"table\" WHERE \"internal_name\" = '" + table + "'";
        return transactionHandler.executeUpdate( sql ) == 1;
    }


    /**
     * Deletes all column records from the catalog.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param table The InternalName of the table.
     * @return A boolean indicating if the column records have been successfully deleted.
     */
    public static boolean deleteColumnsFromTable( XATransactionHandler transactionHandler, InternalName table ) throws SQLException {
        String sql = "DELETE FROM \"column\" WHERE \"table\" = '" + table + "'";
        return transactionHandler.executeUpdate( sql ) >= 1;
    }


    /**
     * Sets a chunk column entry
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param table The unique internal name of the table.
     * @param column The unique internal name of the column.
     * @return A boolean indicating if the chunk column entry was successfully created.
     */
    static boolean setChunkColumn( TransactionHandler transactionHandler, InternalName table, InternalName column ) throws SQLException {
        // insert database record
        String insertSql = "UPDATE \"table\" SET \"chunk_column\" = '" + column + "' WHERE \"internal_name\" = '" + table + "'";
        return transactionHandler.executeUpdate( insertSql ) == 1;
    }


    /**
     * Sets a chunk size entry
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param table The unique internal name of the table.
     * @param size The chunk size.
     * @return A boolean indicating if the chunk size entry was successfully created.
     */
    static boolean setChunkSize( TransactionHandler transactionHandler, InternalName table, Integer size ) throws SQLException {
        // insert database record
        String insertSql = "UPDATE \"table\" SET \"chunk_size\" = " + size + " WHERE \"internal_name\" = '" + table + "'";
        return transactionHandler.executeUpdate( insertSql ) == 1;
    }


    /**
     * This searches for the smallest unused internal name for a database, which allows reusing internal names of dropped databases. This is required because the internal names have a limited length.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @return An internal name for a database
     */
    static InternalName getFreeDatabaseInternalName( TransactionHandler transactionHandler ) throws SQLException, GenericCatalogException, ExceedsMaximumNumberOfDatabasesException {
        int internal_name_part;
        String sql = "SELECT TOP 1 \"t1\".\"internal_name_part\" + 1 FROM \"database\" \"t1\" WHERE NOT EXISTS(SELECT * FROM \"database\" \"t2\" WHERE \"t2\".\"internal_name_part\" = \"t1\".\"internal_name_part\" + 1) ORDER BY \"t1\".\"internal_name_part\";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                internal_name_part = rs.getInt( 1 );
            } else {
                internal_name_part = 1;
            }
        }
        try {
            return new InternalName( internal_name_part, 0, 0, 0 );
        } catch ( ExceedsMaximumNumberOfColumnsException | ExceedsMaximumNumberOfTablesException | ExceedsMaximumNumberOfSchemasException e ) { // Only throw ExceedsMaximumNumberOfDatabasesException
            throw new GenericCatalogException( "Caught an exceeds exception but there is nothing which can be exceeded. This makes no sense..." );
        }
    }


    /**
     * This searches for the smallest unused internal name for a schema, which allows reusing internal names of dropped schemas. This is required because the internal names have a limited length.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param database The InternalName of the database
     * @return An internal name for a schema
     */
    static InternalName getFreeSchemaInternalName( TransactionHandler transactionHandler, InternalName database ) throws SQLException, GenericCatalogException, ExceedsMaximumNumberOfSchemasException {
        int internal_name_part;
        String sql = "SELECT TOP 1 \"t1\".\"internal_name_part\" + 1 FROM \"schema\" \"t1\" WHERE NOT EXISTS(SELECT * FROM \"schema\" \"t2\" WHERE \"t2\".\"internal_name_part\" = \"t1\".\"internal_name_part\" + 1 AND \"database\" = '" + database + "') AND \"database\" = '" + database + "' ORDER BY \"t1\".\"internal_name_part\";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                internal_name_part = rs.getInt( 1 );
            } else {
                internal_name_part = 1;
            }
        }
        try {
            return new InternalName( database.getDatabasePart(), internal_name_part, 0, 0 );
        } catch ( ExceedsMaximumNumberOfColumnsException | ExceedsMaximumNumberOfTablesException | ExceedsMaximumNumberOfDatabasesException e ) { // Only throw ExceedsMaximumNumberOfSchemasException
            throw new GenericCatalogException( "Caught an exceeds exception but there is nothing which can be exceeded. This makes no sense..." );
        }
    }


    /**
     * This searches for the smallest unused internal name for a table, which allows reusing internal names of dropped tables. This is required because the internal names have a limited length.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param schema The InternalName of the schema
     * @return An internal name for a table
     */
    static InternalName getFreeTableInternalName( TransactionHandler transactionHandler, InternalName schema ) throws SQLException, GenericCatalogException, ExceedsMaximumNumberOfTablesException {
        int internal_name_part;
        String sql = "SELECT TOP 1 \"t1\".\"internal_name_part\" + 1 FROM \"table\" \"t1\" WHERE NOT EXISTS(SELECT * FROM \"table\" \"t2\" WHERE \"t2\".\"internal_name_part\" = \"t1\".\"internal_name_part\" + 1 AND \"schema\" = '" + schema + "') AND \"schema\" = '" + schema + "' ORDER BY \"t1\".\"internal_name_part\";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                internal_name_part = rs.getInt( 1 );
            } else {
                internal_name_part = 1;
            }
        }
        try {
            return new InternalName( schema.getDatabasePart(), schema.getSchemaPart(), internal_name_part, 0 );
        } catch ( ExceedsMaximumNumberOfColumnsException | ExceedsMaximumNumberOfSchemasException | ExceedsMaximumNumberOfDatabasesException e ) { // Only throw ExceedsMaximumNumberOfTablesException
            throw new GenericCatalogException( "Caught an exceeds exception but there is nothing which can be exceeded. This makes no sense..." );
        }
    }


    /**
     * This searches for the smallest unused internal name for a column, which allows reusing internal names of dropped columns. This is required because the internal names have a limited length.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param table The InternalName of the table
     * @return An internal name for a column
     */
    static InternalName getFreeColumnInternalName( TransactionHandler transactionHandler, InternalName table ) throws SQLException, GenericCatalogException, ExceedsMaximumNumberOfColumnsException {
        int internal_name_part;
        String sql = "SELECT TOP 1 \"t1\".\"internal_name_part\" + 1 FROM \"column\" \"t1\" WHERE NOT EXISTS(SELECT * FROM \"column\" \"t2\" WHERE \"t2\".\"internal_name_part\" = \"t1\".\"internal_name_part\" + 1  AND \"table\" = '" + table + "') AND \"table\" = '" + table + "' ORDER BY \"t1\".\"internal_name_part\";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                internal_name_part = rs.getInt( 1 );
            } else {
                internal_name_part = 1;
            }
        }
        try {
            return new InternalName( table.getDatabasePart(), table.getSchemaPart(), table.getTablePart(), internal_name_part );
        } catch ( ExceedsMaximumNumberOfTablesException | ExceedsMaximumNumberOfSchemasException | ExceedsMaximumNumberOfDatabasesException e ) { // Only throw ExceedsMaximumNumberOfTablesException
            throw new GenericCatalogException( "Caught an exceeds exception but there is nothing which can be exceeded. This makes no sense..." );
        }
    }


    /**
     * Returns the id of the corresponding user.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param username The username
     * @return The user id
     */
    static int getUserId( TransactionHandler transactionHandler, String username ) throws SQLException, UnknownUserException {
        String sql = "SELECT \"id\" FROM \"user\" WHERE \"username\" = '" + username + "';";
        int user;
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                user = rs.getInt( 1 );
            } else {
                throw new UnknownUserException( username );
            }
        }
        return user;
    }


    /**
     * Returns the internal name of the corresponding database
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param databaseName Tha name of the database
     * @return The internal name of the database
     */
    static InternalName getDatabaseInternalName( TransactionHandler transactionHandler, String databaseName ) throws SQLException, UnknownDatabaseException {
        String sql = "SELECT \"internal_name\" FROM \"database\" WHERE \"name\" = '" + databaseName + "';";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                return new InternalName( rs.getString( "internal_name" ) );
            } else {
                throw new UnknownDatabaseException( databaseName );
            }
        }
    }


    /**
     * Returns the internal name of the corresponding schema
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param schemaName The name of the schema
     * @param database The InternalName of the database
     * @return The internal name of the schema
     */
    static InternalName getSchemaInternalName( TransactionHandler transactionHandler, String schemaName, InternalName database ) throws SQLException, UnknownSchemaException {
        String sql = "SELECT \"internal_name\" FROM \"schema\" WHERE \"name\" = '" + schemaName + "' AND \"database\" = '" + database + "';";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                return new InternalName( rs.getString( "internal_name" ) );
            } else {
                throw new UnknownSchemaException( schemaName, database );
            }
        }
    }


    /**
     * Returns the id of the corresponding table
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param tableName The name of the table
     * @param schema The InternalName of the schema
     * @return The internal name of the table
     */
    static InternalName getTableInternalName( TransactionHandler transactionHandler, String tableName, InternalName schema ) throws SQLException, UnknownTableException {
        String sql = "SELECT \"internal_name\" FROM \"table\" WHERE \"name\" = '" + tableName + "' AND \"schema\" = '" + schema + "';";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                return new InternalName( rs.getString( "internal_name" ) );
            } else {
                throw new UnknownTableException( tableName, schema );
            }
        }
    }


    /**
     * Returns the id of the corresponding column
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param columnName The name of the column
     * @param table The InternalName of the table
     * @return The internal name of the column
     */
    static InternalName getColumnInternalName( TransactionHandler transactionHandler, String columnName, InternalName table ) throws SQLException, UnknownColumnException {
        String sql = "SELECT \"internal_name\" FROM \"column\" WHERE \"name\" = '" + columnName + "' AND \"table\" = '" + table + "';";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                return new InternalName( rs.getString( "internal_name" ) );
            } else {
                throw new UnknownColumnException( columnName, table );
            }
        }
    }


    /**
     * Checks if there is a database with that name
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param databaseName Tha name of the database
     * @return Boolean indicating if the database exists
     */
    static boolean checkIfDatabaseExists( TransactionHandler transactionHandler, String databaseName ) throws SQLException {
        String sql = "SELECT \"internal_name\" FROM \"database\" WHERE \"name\" = '" + databaseName + "';";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Checks if there is a schema with that name in the specified database
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param schemaName Tha name of the schema
     * @param database The internal name of the database
     * @return Boolean indicating if the schema exists
     */
    static boolean checkIfSchemaExists( TransactionHandler transactionHandler, String schemaName, InternalName database ) throws SQLException {
        String sql = "SELECT \"internal_name\" FROM \"schema\" WHERE \"name\" = '" + schemaName + "' AND \"database\" = '" + database + "';";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Checks if there is a table with that name in the specified schema
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param tableName Tha name of the table
     * @param schema The internal name of the schema
     * @return Boolean indicating if the table exists
     */
    static boolean checkIfTableExists( TransactionHandler transactionHandler, String tableName, InternalName schema ) throws SQLException {
        String sql = "SELECT \"internal_name\" FROM \"table\" WHERE \"name\" = '" + tableName + "' AND \"schema\" = '" + schema + "';";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Checks if there is a column with that name in the specified table
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     * @param columnName The name of the column
     * @param table The internal name of the table
     * @return Boolean indicating if the table exists
     */
    static boolean checkIfColumnExists( TransactionHandler transactionHandler, String columnName, InternalName table ) throws SQLException {
        String sql = "SELECT \"internal_name\" FROM \"column\" WHERE \"name\" = '" + columnName + "' AND \"table\" = '" + table + "';";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                return true;
            }
        }
        return false;
    }


    /**
     * Gets the information regarding a column
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param internalName The InternalName of the column
     * @return A CatalogColumn object representing the column
     */
    static CatalogColumn getColumn( TransactionHandler transactionHandler, InternalName internalName ) throws SQLException, UnknownTypeException, UnknownEncodingException, UnknownTableTypeException, UnknownCollationException, UnknownColumnException {
        String sql = "SELECT \"internal_name\", \"name\", \"position\", \"type\", \"length\", \"precision\", \"nullable\", \"encoding\", \"collation\", \"autoincrement_start_value\", \"autoincrement_next_value\", \"force_default\", \"default_value\" FROM \"column\" WHERE \"internal_name\" = '" + internalName.toString() + "';";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                Serializable defaultValue = null;
                //Blob blob = rs.getBlob( 12 );
                //if ( blob != null ) {
                //    defaultValue = blob.toString();
                //}
                return new CatalogColumn(
                        new InternalName( rs.getString( 1 ) ),
                        rs.getString( 2 ),
                        rs.getInt( 3 ),
                        PolySqlType.getByTypeCode( rs.getInt( 4 ) ),
                        rs.getInt( 5 ),
                        rs.getInt( 6 ),
                        rs.getBoolean( 7 ),
                        Encoding.getById( rs.getInt( 8 ) ),
                        Collation.getById( rs.getInt( 9 ) ),
                        rs.getLong( 10 ),
                        rs.getLong( 11 ),
                        defaultValue,
                        rs.getBoolean( 13 )
                );
            } else {
                throw new UnknownColumnException( internalName );
            }
        }
    }


    /**
     * Gets the information regarding a table
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param internalName The InternalName of the table
     * @return A CatalogTable object representing the table
     */
    static CatalogTable getTable( TransactionHandler transactionHandler, InternalName internalName ) throws SQLException, UnknownEncodingException, UnknownTableTypeException, UnknownCollationException, UnknownTableException {
        String sql = "SELECT \"internal_name\", \"name\", \"owner\", \"encoding\", \"collation\", \"type\", \"definition\", \"chunk_column\", \"chunk_size\" FROM \"table\" WHERE \"internal_name\" = '" + internalName.toString() + "';";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                InternalName chunkColumn = null;
                if ( rs.getString( 8 ) != null && rs.getString( 8 ).length() == 10 ) {
                    chunkColumn = new InternalName( rs.getString( 8 ) );
                }
                Integer chunkSize = null;
                if ( rs.getObject( 9 ) instanceof Integer ) {
                    chunkSize = (Integer) rs.getObject( 9 );
                }
                return new CatalogTable(
                        new InternalName( rs.getString( 1 ) ),
                        rs.getString( 2 ),
                        rs.getInt( 3 ),
                        Encoding.getById( rs.getInt( 4 ) ),
                        Collation.getById( rs.getInt( 5 ) ),
                        TableType.getById( rs.getInt( 6 ) ),
                        rs.getString( 7 ),
                        chunkColumn,
                        chunkSize
                );
            } else {
                throw new UnknownTableException( internalName );
            }
        }
    }


    /**
     * Gets the information regarding a schema
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param internalName The InternalName of the schema
     * @return A CatalogSchema object representing the schema
     */
    static CatalogSchema getSchema( TransactionHandler transactionHandler, InternalName internalName ) throws SQLException, UnknownEncodingException, UnknownCollationException, UnknownSchemaException, UnknownTableTypeException {
        String sql = "SELECT \"internal_name\", \"name\", \"owner\", \"encoding\", \"collation\" FROM \"schema\" WHERE \"internal_name\" = '" + internalName.toString() + "';";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                return new CatalogSchema(
                        new InternalName( rs.getString( 1 ) ),
                        rs.getString( 2 ),
                        rs.getInt( 3 ),
                        Encoding.getById( rs.getInt( 4 ) ),
                        Collation.getById( rs.getInt( 5 ) )
                );
            } else {
                throw new UnknownSchemaException( internalName );
            }
        }
    }


    /**
     * Gets the information regarding a database
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param internalName The InternalName of the database
     * @return A CatalogDatabase object representing the database
     */
    static CatalogDatabase getDatabase( TransactionHandler transactionHandler, InternalName internalName ) throws SQLException, UnknownEncodingException, UnknownCollationException, UnknownDatabaseException, UnknownTableTypeException {
        String sql = "SELECT \"internal_name\", \"name\", \"owner\", \"encoding\", \"collation\", \"connection_limit\" FROM \"database\" WHERE \"internal_name\" = '" + internalName.toString() + "';";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            if ( rs.next() ) {
                return new CatalogDatabase(
                        new InternalName( rs.getString( 1 ) ),
                        rs.getString( 2 ),
                        rs.getInt( 3 ),
                        Encoding.getById( rs.getInt( 4 ) ),
                        Collation.getById( rs.getInt( 5 ) ),
                        rs.getInt( 6 )
                );
            } else {
                throw new UnknownDatabaseException( internalName );
            }
        }
    }


    public static List<CatalogColumn> getAllColumnsOfTable( TransactionHandler transactionHandler, InternalName tableInternalName ) throws SQLException, UnknownTableException, UnknownTypeException, UnknownEncodingException, UnknownTableTypeException, UnknownCollationException {
        String sql = "SELECT \"internal_name\", \"name\", \"position\", \"type\", \"length\", \"precision\", \"nullable\", \"encoding\", \"collation\", \"autoincrement_start_value\", \"autoincrement_next_value\", \"force_default\", \"default_value\" FROM \"column\" WHERE \"table\" = '" + tableInternalName.toString() + "' ORDER BY \"position\";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            List<CatalogColumn> columns = new LinkedList<>();
            while ( rs.next() ) {
                Serializable defaultValue = null;
                //Blob blob = rs.getBlob( 12 );
                //if ( blob != null ) {
                //    defaultValue = blob.toString();
                //}
                columns.add( new CatalogColumn(
                        new InternalName( rs.getString( 1 ) ),
                        rs.getString( 2 ),
                        rs.getInt( 3 ),
                        PolySqlType.getByTypeCode( rs.getInt( 4 ) ),
                        rs.getInt( 5 ),
                        rs.getInt( 6 ),
                        rs.getBoolean( 7 ),
                        Encoding.getById( rs.getInt( 8 ) ),
                        Collation.getById( rs.getInt( 9 ) ),
                        rs.getLong( 10 ),
                        rs.getLong( 11 ),
                        defaultValue,
                        rs.getBoolean( 13 )
                ) );
            }

            if ( columns.size() == 0 ) {
                throw new UnknownTableException( tableInternalName );
            }

            return columns;
        }
    }


    /**
     * Get all tables (independent of schema and database)
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @return A List of CatalogTables
     */
    static List<CatalogTable> getAllTables( TransactionHandler transactionHandler ) throws SQLException, UnknownEncodingException, UnknownTableTypeException, UnknownCollationException {
        String sql = "SELECT \"internal_name\", \"name\", \"owner\", \"encoding\", \"collation\", \"type\", \"definition\", \"chunk_column\", \"chunk_size\" FROM \"table\";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            List<CatalogTable> list = new LinkedList<>();
            while ( rs.next() ) {
                InternalName chunkColumn = null;
                if ( rs.getString( 8 ) != null && rs.getString( 8 ).length() == 10 ) {
                    chunkColumn = new InternalName( rs.getString( 8 ) );
                }
                Integer chunkSize = null;
                if ( rs.getObject( 9 ) instanceof Integer ) {
                    chunkSize = (Integer) rs.getObject( 9 );
                }
                list.add( new CatalogTable(
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
            }
            return list;
        }
    }


    /**
     * Generates a X/Open transaction identifier used for distributed transactions
     *
     * @return A XID
     */
    static PolyXid getXid() {
        return new PolyXid( JDBCXID.getUniqueXid( (int) Thread.currentThread().getId() ) );
    }

}
