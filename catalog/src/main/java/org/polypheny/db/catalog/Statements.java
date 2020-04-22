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


import static org.hsqldb.persist.HsqlProperties.indexName;

import com.google.gson.Gson;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringEscapeUtils;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.Catalog.ForeignKeyOption;
import org.polypheny.db.catalog.Catalog.IndexType;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogDefaultValue;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogStore;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintTypeException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyException;
import org.polypheny.db.catalog.exceptions.UnknownForeignKeyOptionException;
import org.polypheny.db.catalog.exceptions.UnknownIndexException;
import org.polypheny.db.catalog.exceptions.UnknownIndexTypeException;
import org.polypheny.db.catalog.exceptions.UnknownKeyException;
import org.polypheny.db.catalog.exceptions.UnknownPlacementTypeException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaTypeException;
import org.polypheny.db.catalog.exceptions.UnknownStoreException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.catalog.exceptions.UnknownTableTypeException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.UnknownTypeException;


/**
 * This class provides methods to interact with the catalog database. All SQL-stuff should be in this class.
 */
@Slf4j
final class Statements {

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
        log.debug( "Creating the catalog schema" );
        boolean result = ScriptRunner.runScript( new InputStreamReader( Statements.class.getResourceAsStream( "/catalogSchema.sql" ) ), transactionHandler, false );
        if ( !result ) {
            log.error( "Exception while creating catalog schema" );
        }
    }


    /**
     * Drop the catalog schema.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     */
    static void dropSchema( TransactionHandler transactionHandler ) {
        log.debug( "Dropping the catalog schema" );
        try {
            transactionHandler.execute( "DROP TABLE IF EXISTS \"user\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"database\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"schema\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"table\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"column\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"default_value\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"column_privilege\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"table_privilege\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"schema_privilege\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"database_privilege\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"global_privilege\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"store\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"data_placement\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"key\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"key_column\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"foreign_key\";" );
            transactionHandler.execute( "DROP TABLE IF EXISTS \"index\";" );
        } catch ( SQLException e ) {
            log.error( "Exception while dropping catalog schema", e );
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

            System.out.println( "Store:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"store\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Column Placement:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"column_placement\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Key:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"key\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Key Column:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"key_column\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Foreign Key:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"foreign_key\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );

            System.out.println( "Index:" );
            resultSet = transactionHandler.executeSelect( "SELECT * FROM \"index\";" );
            System.out.println( TablePrinter.processResultSet( resultSet ) );
        } catch ( SQLException e ) {
            log.error( "Caught exception while creating catalog schema!", e );
        }
    }


    /**
     * Export the current schema of Polypheny-DB as bunch of PolySQL statements. Currently printed to stdout for debugging purposes.
     *
     * !!! This method is only partially implemented !!!
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the (distributed) transaction.
     */
    /*static void export( TransactionHandler transactionHandler ) {
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
                        long scale = columnResultSet.getLong( "scale" );
                        boolean nullable = columnResultSet.getBoolean( "nullable" );
                        Collation collation = Collation.getById( columnResultSet.getInt( "collation" ) );
                        java.io.Serializable defaultValue = columnResultSet.getString( "default_value" );
                        int autoIncrementStartValue = columnResultSet.getInt( "autoIncrement_start_value" );

                        System.out.print( "    " + columnName + " " + columnType.name() );

                        // type arguments (length and scale)
                        if ( length != 0 ) {
                            System.out.print( "(" + length );
                            if ( scale != 0 ) {
                                System.out.print( ", " + scale );
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

                        // collation
                        System.out.print( " COLLATION " + collation.name() );

                        System.out.println( "," );
                    }
                    System.out.println( ");" );
                }
            }
        } catch ( SQLException | UnknownTableTypeException | UnknownTypeException | UnknownCollationException e ) {
            log.error( "Caught exception while exporting catalog!", e );
        }
    }*/

    // -------------------------------------------------------------------------------------------------------------------------------------------------------
    //
    //                                                                     Databases
    //
    // -------------------------------------------------------------------------------------------------------------------------------------------------------


    private static List<CatalogDatabase> databaseFilter( TransactionHandler transactionHandler, String filter ) throws GenericCatalogException {
        String sql = "SELECT d.\"id\", d.\"name\", u.\"id\", u.\"username\", s.\"id\", s.\"name\" FROM \"database\" d, \"schema\" s, \"user\" u WHERE d.\"owner\" = u.\"id\" AND d.\"default_schema\" = s.\"id\"" + filter + ";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            List<CatalogDatabase> list = new LinkedList<>();
            while ( rs.next() ) {
                list.add( new CatalogDatabase(
                        getLong( rs, 1 ),
                        rs.getString( 2 ),
                        getInt( rs, 3 ),
                        rs.getString( 4 ),
                        getLongOrNull( rs, 5 ),
                        rs.getString( 6 )
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
    static List<CatalogDatabase> getDatabases( TransactionHandler transactionHandler, Pattern namePattern ) throws GenericCatalogException {
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
    static CatalogDatabase getDatabase( TransactionHandler transactionHandler, String databaseName ) throws UnknownDatabaseException, GenericCatalogException {
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
    static CatalogDatabase getDatabase( TransactionHandler transactionHandler, long databaseId ) throws UnknownDatabaseException, GenericCatalogException {
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


    private static List<CatalogSchema> schemaFilter( TransactionHandler transactionHandler, String filter ) throws UnknownSchemaTypeException, GenericCatalogException {
        String sql = "SELECT s.\"id\", s.\"name\", d.\"id\", d.\"name\", u.\"id\", u.\"username\", s.\"type\" FROM \"schema\" s, \"database\" d, \"user\" u WHERE s.\"database\" = d.\"id\" AND s.\"owner\" = u.\"id\"" + filter + ";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            List<CatalogSchema> list = new LinkedList<>();
            while ( rs.next() ) {
                list.add( new CatalogSchema(
                        getLong( rs, 1 ),
                        rs.getString( 2 ),
                        getLong( rs, 3 ),
                        rs.getString( 4 ),
                        getInt( rs, 5 ),
                        rs.getString( 6 ),
                        SchemaType.getById( getInt( rs, 7 ) )
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
    static List<CatalogSchema> getSchemas( TransactionHandler transactionHandler, long databaseId, Pattern schemaNamePattern ) throws GenericCatalogException, UnknownSchemaTypeException {
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
    static List<CatalogSchema> getSchemas( TransactionHandler transactionHandler, Pattern databaseNamePattern, Pattern schemaNamePattern ) throws GenericCatalogException, UnknownSchemaTypeException {
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
    static CatalogSchema getSchema( TransactionHandler transactionHandler, long schemaId ) throws GenericCatalogException, UnknownSchemaTypeException, UnknownSchemaException {
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
    static CatalogSchema getSchema( TransactionHandler transactionHandler, long databaseId, String schemaName ) throws GenericCatalogException, UnknownSchemaTypeException, UnknownSchemaException {
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
    static CatalogSchema getSchema( TransactionHandler transactionHandler, String databaseName, String schemaName ) throws GenericCatalogException, UnknownSchemaTypeException, UnknownSchemaException {
        String filter = " AND d.\"name\" = '" + databaseName + "' AND s.\"name\" = '" + schemaName + "'";
        List<CatalogSchema> list = schemaFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownSchemaException( databaseName, schemaName );
        }
        return list.get( 0 );
    }


    static long addSchema( XATransactionHandler transactionHandler, String name, long databaseId, int ownerId, SchemaType schemaType ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "database", "" + databaseId );
        data.put( "name", quoteString( name ) );
        data.put( "owner", "" + ownerId );
        data.put( "type", "" + schemaType.getId() );
        return insertHandler( transactionHandler, "schema", data );
    }


    static void renameSchema( XATransactionHandler transactionHandler, long schemaId, String name ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "name", quoteString( name ) );
        Map<String, String> where = new LinkedHashMap<>();
        where.put( "id", "" + schemaId );
        updateHandler( transactionHandler, "schema", data, where );
    }


    static void setSchemaOwner( XATransactionHandler transactionHandler, long schemaId, long ownerId ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "owner", "" + ownerId );
        Map<String, String> where = new LinkedHashMap<>();
        where.put( "id", "" + schemaId );
        updateHandler( transactionHandler, "schema", data, where );
    }


    static void deleteSchema( XATransactionHandler transactionHandler, long schemaId ) throws GenericCatalogException {
        String sql = "DELETE FROM " + quoteIdentifier( "schema" ) + " WHERE " + quoteIdentifier( "id" ) + " = " + schemaId;
        try {
            int rowsEffected = transactionHandler.executeUpdate( sql );
            if ( rowsEffected != 1 ) {
                throw new GenericCatalogException( "Expected only one effected row, but " + rowsEffected + " have been effected." );
            }
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }




    // -------------------------------------------------------------------------------------------------------------------------------------------------------
    //
    //                                                                     Tables
    //
    // -------------------------------------------------------------------------------------------------------------------------------------------------------


    private static List<CatalogTable> tableFilter( TransactionHandler transactionHandler, String filter ) throws UnknownTableTypeException, GenericCatalogException {
        final String sql = "SELECT t.\"id\", t.\"name\", s.\"id\", s.\"name\", d.\"id\", d.\"name\", u.\"id\", u.\"username\", t.\"type\", t.\"definition\", t.\"primary_key\" FROM \"table\" t, \"schema\" s, \"database\" d, \"user\" u WHERE t.\"schema\" = s.\"id\" AND s.\"database\" = d.\"id\" AND t.\"owner\" = u.\"id\"" + filter + ";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            List<CatalogTable> list = new LinkedList<>();
            while ( rs.next() ) {
                list.add( new CatalogTable(
                        getLong( rs, 1 ),
                        rs.getString( 2 ),
                        getLong( rs, 3 ),
                        rs.getString( 4 ),
                        getLong( rs, 5 ),
                        rs.getString( 6 ),
                        getInt( rs, 7 ),
                        rs.getString( 8 ),
                        TableType.getById( getInt( rs, 9 ) ),
                        rs.getString( 10 ),
                        getLongOrNull( rs, 11 )
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
    static List<CatalogTable> getTables( TransactionHandler transactionHandler, long schemaId, Pattern tableNamePattern ) throws GenericCatalogException, UnknownTableTypeException {
        String filter = " AND s.\"id\" = " + schemaId;
        if ( tableNamePattern != null ) {
            filter += " AND t.\"name\" LIKE '" + tableNamePattern.pattern + "'";
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
    static List<CatalogTable> getTables( TransactionHandler transactionHandler, long databaseId, Pattern schemaNamePattern, Pattern tableNamePattern ) throws GenericCatalogException, UnknownTableTypeException {
        String filter = " AND s.\"database\" = " + databaseId;
        if ( tableNamePattern != null ) {
            filter += " AND t.\"name\" LIKE '" + tableNamePattern.pattern + "'";
        }
        if ( schemaNamePattern != null ) {
            filter += " AND s.\"name\" LIKE '" + schemaNamePattern.pattern + "'";
        }
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
    static List<CatalogTable> getTables( TransactionHandler transactionHandler, Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern ) throws GenericCatalogException, UnknownTableTypeException {
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
        return tableFilter( transactionHandler, filter );
    }


    /**
     * Get the specified table
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param tableId The id of the table
     * @return A CatalogTable
     */
    static CatalogTable getTable( TransactionHandler transactionHandler, long tableId ) throws GenericCatalogException, UnknownTableTypeException, UnknownTableException {
        String filter = " AND \"id\" = " + tableId;
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
    static CatalogTable getTable( TransactionHandler transactionHandler, long schemaId, String tableName ) throws GenericCatalogException, UnknownTableTypeException, UnknownTableException {
        String filter = " AND \"schema\" = " + schemaId + " AND \"name\" = '" + tableName + "'";
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
    static CatalogTable getTable( TransactionHandler transactionHandler, long databaseId, String schemaName, String tableName ) throws GenericCatalogException, UnknownTableTypeException, UnknownTableException {
        String filter = " AND s.\"database\" = " + databaseId + " AND s.\"name\" = '" + schemaName + "' AND t.\"name\" = '" + tableName + "'";
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
    static CatalogTable getTable( TransactionHandler transactionHandler, String databaseName, String schemaName, String tableName ) throws GenericCatalogException, UnknownTableTypeException, UnknownTableException {
        String filter = " AND d.\"name\" = '" + databaseName + "' AND s.\"name\" = '" + schemaName + "' AND t.\"name\" = '" + tableName + "'";
        List<CatalogTable> list = tableFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownTableException( databaseName, schemaName, tableName );
        }
        return list.get( 0 );
    }


    static long addTable( XATransactionHandler transactionHandler, String name, long schemaId, int ownerId, TableType tableType, String definition ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "schema", "" + schemaId );
        data.put( "name", quoteString( name ) );
        data.put( "owner", "" + ownerId );
        data.put( "type", "" + tableType.getId() );
        data.put( "definition", quoteString( definition ) );
        return insertHandler( transactionHandler, "table", data );
    }


    static void renameTable( XATransactionHandler transactionHandler, long tableId, String name ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "name", quoteString( name ) );
        Map<String, String> where = new LinkedHashMap<>();
        where.put( "id", "" + tableId );
        updateHandler( transactionHandler, "table", data, where );
    }


    static void setTableOwner( XATransactionHandler transactionHandler, long tableId, int ownerId ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "owner", "" + ownerId );
        Map<String, String> where = new LinkedHashMap<>();
        where.put( "id", "" + tableId );
        updateHandler( transactionHandler, "table", data, where );
    }


    static void deleteTable( XATransactionHandler transactionHandler, long tableId ) throws GenericCatalogException {
        String sql = "DELETE FROM " + quoteIdentifier( "table" ) + " WHERE " + quoteIdentifier( "id" ) + " = " + tableId;
        try {
            int rowsEffected = transactionHandler.executeUpdate( sql );
            if ( rowsEffected != 1 ) {
                throw new GenericCatalogException( "Expected only one effected row, but " + rowsEffected + " have been effected." );
            }
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }


    static void setPrimaryKey( XATransactionHandler transactionHandler, long tableId, Long keyId ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "primary_key", keyId == null ? null : "" + keyId );
        Map<String, String> where = new LinkedHashMap<>();
        where.put( "id", "" + tableId );
        updateHandler( transactionHandler, "table", data, where );
    }


    // -------------------------------------------------------------------------------------------------------------------------------------------------------
    //
    //                                                                     Columns
    //
    // -------------------------------------------------------------------------------------------------------------------------------------------------------


    private static List<CatalogColumn> columnFilter( TransactionHandler transactionHandler, String filter ) throws GenericCatalogException, UnknownTypeException, UnknownCollationException {
        String sql = "SELECT c.\"id\", c.\"name\", t.\"id\", t.\"name\", s.\"id\", s.\"name\", d.\"id\", d.\"name\", c.\"position\", c.\"type\", c.\"collections_type\", c.\"length\", c.\"scale\", c.\"dimension\", c.\"cardinality\", c.\"nullable\", c.\"collation\" FROM \"column\" c, \"table\" t, \"schema\" s, \"database\" d WHERE c.\"table\" = t.\"id\" AND t.\"schema\" = s.\"id\"  AND s.\"database\" = d.\"id\"" + filter + " ORDER BY d.\"id\", s.\"id\", t.\"id\", c.\"position\";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            List<CatalogColumn> list = new LinkedList<>();
            while ( rs.next() ) {
                String defaultValueSql = "SELECT dv.\"column\", dv.\"type\", dv.\"value\", dv.\"function_name\" FROM \"default_value\" dv where dv.\"column\" = " + getLongOrNull( rs, 1 );
                CatalogDefaultValue defaultValue = null;
                try ( ResultSet rsdv = transactionHandler.executeSelect( defaultValueSql ) ) {
                    if ( rsdv.next() ) {
                        defaultValue = new CatalogDefaultValue(
                                getLong( rsdv, 1 ),
                                PolyType.get( rsdv.getString( 2 ) ),
                                rsdv.getString( 3 ),
                                rsdv.getString( 4 )
                        );
                    }
                }

                Collation collation = null;
                Integer collationId = getIntOrNull( rs, 17 );
                if ( collationId != null ) {
                    collation = Collation.getById( collationId );
                }

                final PolyType collectionsType = rs.getString( 11 ) == null ?
                        null : PolyType.get( rs.getString( 11 ) );
                CatalogColumn c = new CatalogColumn(
                        getLong( rs, 1 ),
                        rs.getString( 2 ),
                        getLong( rs, 3 ),
                        rs.getString( 4 ),
                        getLong( rs, 5 ),
                        rs.getString( 6 ),
                        getLong( rs, 7 ),
                        rs.getString( 8 ),
                        getInt( rs, 9 ),
                        PolyType.get( rs.getString( 10 ) ),
                        collectionsType,
                        getIntOrNull( rs, 12 ),
                        getIntOrNull( rs, 13 ),
                        getIntOrNull( rs, 14 ),
                        getIntOrNull( rs, 15 ),
                        rs.getBoolean( 16 ),
                        collation,
                        defaultValue
                );
                //System.out.println(String.format("Statements column filter: %s %s", c.name, c.type.getName()));
                list.add( c );
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
    static List<CatalogColumn> getColumns( TransactionHandler transactionHandler, long tableId ) throws UnknownCollationException, GenericCatalogException, UnknownTypeException {
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
    static List<CatalogColumn> getColumns( TransactionHandler transactionHandler, Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern, Pattern columnNamePattern ) throws UnknownCollationException, GenericCatalogException, UnknownTypeException {
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
        if ( databaseNamePattern != null ) {
            filter += " AND d.\"name\" LIKE '" + databaseNamePattern.pattern + "'";
        }
        return columnFilter( transactionHandler, filter );
    }


    static CatalogColumn getColumn( XATransactionHandler transactionHandler, long columnId ) throws UnknownCollationException, UnknownTypeException, GenericCatalogException, UnknownColumnException {
        String filter = " AND c.\"id\" = " + columnId;
        List<CatalogColumn> list = columnFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownColumnException( columnId );
        }
        return list.get( 0 );
    }


    /**
     * Returns the column with the specified table.
     *
     * @param transactionHandler The transaction handler which allows accessing the database and manages the transaction.
     * @param tableId The id of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     */
    static CatalogColumn getColumn( TransactionHandler transactionHandler, long tableId, String columnName ) throws UnknownCollationException, GenericCatalogException, UnknownTypeException, UnknownColumnException {
        String filter = " AND c.\"table\" = " + tableId + " AND c.\"name\" = '" + columnName + "'";
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
    static CatalogColumn getColumn( TransactionHandler transactionHandler, String databaseName, String schemaName, String tableName, String columnName ) throws UnknownCollationException, GenericCatalogException, UnknownTypeException, UnknownColumnException {
        String filter = " AND d.\"name\" = '" + databaseName + "' AND s.\"name\" = '" + schemaName + "' AND t.\"name\" = '" + tableName + "' AND c.\"name\" = '" + columnName + "'";
        List<CatalogColumn> list = columnFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownColumnException( databaseName, schemaName, tableName, columnName );
        }
        return list.get( 0 );
    }


    static long addColumn( XATransactionHandler transactionHandler, String name, long tableId, int position, PolyType type, PolyType collectionsType, Integer length, Integer scale, Integer dimension, Integer cardinality, boolean nullable, Collation collation ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "table", "" + tableId );
        data.put( "name", quoteString( name ) );
        data.put( "position", "" + position );
        data.put( "type", "" + quoteString( type.getName() ) );
        if ( collectionsType != null ) {
            data.put( "collections_type", quoteString( collectionsType.getName() ) );
        }
        data.put( "length", length == null ? null : "" + length );
        data.put( "scale", scale == null ? null : "" + scale );
        data.put( "dimension", dimension == null ? null : "" + dimension );
        data.put( "cardinality", cardinality == null ? null : "" + cardinality );
        data.put( "nullable", "" + nullable );
        if ( collation != null ) {
            data.put( "collation", "" + collation.getId() );
        }
        return insertHandler( transactionHandler, "column", data );
    }


    static void renameColumn( XATransactionHandler transactionHandler, long columnId, String name ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "name", quoteString( name ) );
        Map<String, String> where = new LinkedHashMap<>();
        where.put( "id", "" + columnId );
        updateHandler( transactionHandler, "column", data, where );
    }


    static void setColumnPosition( XATransactionHandler transactionHandler, long columnId, int position ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "position", "" + position );
        Map<String, String> where = new LinkedHashMap<>();
        where.put( "id", "" + columnId );
        updateHandler( transactionHandler, "column", data, where );
    }


    static void setColumnType( XATransactionHandler transactionHandler, long columnId, PolyType type, final Integer length, final Integer scale, Collation collation ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "type", "" + quoteString( type.getName() ) );
        data.put( "length", length == null ? null : "" + length );
        data.put( "scale", scale == null ? null : "" + scale );
        data.put( "collation", collation == null ? null : "" + collation.getId() );
        Map<String, String> where = new LinkedHashMap<>();
        where.put( "id", "" + columnId );
        updateHandler( transactionHandler, "column", data, where );
    }


    static void setNullable( XATransactionHandler transactionHandler, long columnId, boolean nullable ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "nullable", "" + nullable );
        Map<String, String> where = new LinkedHashMap<>();
        where.put( "id", "" + columnId );
        updateHandler( transactionHandler, "column", data, where );
    }


    public static void setCollation( XATransactionHandler transactionHandler, long columnId, Collation collation ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "collation", "" + collation.getId() );
        Map<String, String> where = new LinkedHashMap<>();
        where.put( "id", "" + columnId );
        updateHandler( transactionHandler, "column", data, where );
    }


    static void deleteColumn( XATransactionHandler transactionHandler, long columnId ) throws GenericCatalogException {
        String sql = "DELETE FROM " + quoteIdentifier( "column" ) + " WHERE " + quoteIdentifier( "id" ) + " = " + columnId;
        try {
            int rowsEffected = transactionHandler.executeUpdate( sql );
            if ( rowsEffected != 1 ) {
                throw new GenericCatalogException( "Expected only one effected row, but " + rowsEffected + " have been effected." );
            }
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }


    static void setDefaultValue( XATransactionHandler transactionHandler, long columnId, PolyType type, String defaultValue ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "column", "" + columnId );
        data.put( "type", "" + quoteString( type.getName() ) );
        data.put( "value", quoteString( defaultValue ) );
        insertHandler( transactionHandler, "default_value", data );
    }


    static void deleteDefaultValue( XATransactionHandler transactionHandler, long columnId ) throws GenericCatalogException {
        String sql = "DELETE FROM " + quoteIdentifier( "default_value" ) + " WHERE " + quoteIdentifier( "column" ) + " = " + columnId;
        try {
            int rowsEffected = transactionHandler.executeUpdate( sql );
            if ( rowsEffected > 1 ) {
                throw new GenericCatalogException( "Expected zero or one effected row, but " + rowsEffected + " have been effected." );
            }
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
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
                        getInt( rs, 1 ),
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

    // -------------------------------------------------------------------------------------------------------------------------------------------------------
    //
    //                                                                Store
    //
    // -------------------------------------------------------------------------------------------------------------------------------------------------------


    private static List<CatalogStore> storeFilter( TransactionHandler transactionHandler, String filter ) throws GenericCatalogException {
        String sql = "SELECT \"id\", \"unique_name\", \"adapter\", \"settings\" FROM \"store\"";
        if ( filter.length() > 0 ) {
            sql += " WHERE " + filter;
        }
        sql += ";";
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            List<CatalogStore> list = new LinkedList<>();
            Gson gson = new Gson();
            while ( rs.next() ) {
                @SuppressWarnings("unchecked") Map<String, String> configMap = gson.fromJson( rs.getString( 4 ), Map.class );
                list.add( new CatalogStore(
                        getInt( rs, 1 ),
                        rs.getString( 2 ),
                        rs.getString( 3 ),
                        configMap
                ) );
            }
            return list;
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }


    // Get all Stores
    static List<CatalogStore> getStores( TransactionHandler transactionHandler ) throws GenericCatalogException {
        return storeFilter( transactionHandler, "" );
    }


    // Get Store by store id
    static CatalogStore getStore( XATransactionHandler transactionHandler, int id ) throws GenericCatalogException, UnknownStoreException {
        String filter = " \"id\" = " + id;
        List<CatalogStore> list = storeFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownStoreException( id );
        }
        return list.get( 0 );
    }


    // Get Store by store name
    static CatalogStore getStore( XATransactionHandler transactionHandler, String uniqueName ) throws GenericCatalogException, UnknownStoreException {
        String filter = " \"unique_name\" = " + quoteString( uniqueName );
        List<CatalogStore> list = storeFilter( transactionHandler, filter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownStoreException( uniqueName );
        }
        return list.get( 0 );
    }


    static int addStore( XATransactionHandler transactionHandler, String unique_name, String adapter, Map<String, String> settings ) throws GenericCatalogException {
        Gson gson = new Gson();
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "unique_name", quoteString( unique_name ) );
        data.put( "adapter", quoteString( adapter ) );
        data.put( "settings", quoteString( StringEscapeUtils.escapeSql( gson.toJson( settings ) ) ) );
        return Math.toIntExact(insertHandler( transactionHandler, "store", data ));
    }


    static void deleteStore( XATransactionHandler transactionHandler, long storeId ) throws GenericCatalogException {
        String sql = "DELETE FROM " + quoteIdentifier( "store" ) + " WHERE " + quoteIdentifier( "id" ) + " = " + storeId;
        try {
            int rowsEffected = transactionHandler.executeUpdate( sql );
            if ( rowsEffected != 1 ) {
                throw new GenericCatalogException( "Expected only one effected row, but " + rowsEffected + " have been effected." );
            }
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }

    // -------------------------------------------------------------------------------------------------------------------------------------------------------
    //
    //                                                                Column Placement
    //
    // -------------------------------------------------------------------------------------------------------------------------------------------------------


    private static List<CatalogColumnPlacement> columnPlacementFilter( TransactionHandler transactionHandler, String filter ) throws GenericCatalogException {
        String sql = "SELECT t.\"id\", t.\"name\", c.\"id\", c.\"name\", st.\"id\", st.\"unique_name\", cp.\"type\", cp.\"physical_schema\", cp.\"physical_table\", cp.\"physical_column\" FROM \"column_placement\" cp, \"store\" st, \"table\" t, \"column\" c WHERE cp.\"store\" = st.\"id\" AND cp.\"table\" = t.\"id\" AND cp.\"column\" = c.\"id\"" + filter;
        try ( ResultSet rs = transactionHandler.executeSelect( sql ) ) {
            List<CatalogColumnPlacement> list = new LinkedList<>();
            while ( rs.next() ) {
                list.add( new CatalogColumnPlacement(
                        getLong( rs, 1 ),
                        rs.getString( 2 ),
                        getLong( rs, 3 ),
                        rs.getString( 4 ),
                        getInt( rs, 5 ),
                        rs.getString( 6 ),
                        PlacementType.getById( getInt( rs, 7 ) ),
                        rs.getString( 8 ),
                        rs.getString( 9 ),
                        rs.getString( 10 )
                ) );
            }
            return list;
        } catch ( SQLException | UnknownPlacementTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    static List<CatalogColumnPlacement> getColumnPlacements( XATransactionHandler transactionHandler, long columnId ) throws GenericCatalogException {
        String filter = " AND c.\"id\" = " + columnId;
        return columnPlacementFilter( transactionHandler, filter );
    }


    static List<CatalogColumnPlacement> getColumnPlacementsOfTable( XATransactionHandler transactionHandler, long tableId ) throws GenericCatalogException {
        String filter = " AND t.\"id\" = " + tableId;
        return columnPlacementFilter( transactionHandler, filter );
    }


    static List<CatalogColumnPlacement> getColumnPlacementsOnStore( XATransactionHandler transactionHandler, long storeId ) throws GenericCatalogException {
        String filter = " AND st.\"id\" = " + storeId;
        return columnPlacementFilter( transactionHandler, filter );
    }


    public static List<CatalogColumnPlacement> getColumnPlacementsOnStoreAndSchema( XATransactionHandler transactionHandler, int storeId, long schemaId ) throws GenericCatalogException {
        String filter = " AND st.\"id\" = " + storeId + " AND t.\"schema\" = " + schemaId;
        return columnPlacementFilter( transactionHandler, filter );
    }


    public static void updateColumnPlacementPhysicalNames( XATransactionHandler transactionHandler, int storeId, long columnId, String physicalSchemaName, String physicalTableName, String physicalColumnName ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "physical_schema", quoteString( physicalSchemaName ) );
        data.put( "physical_table", quoteString( physicalTableName ) );
        data.put( "physical_column", quoteString( physicalColumnName ) );
        Map<String, String> where = new LinkedHashMap<>();
        where.put( "column", "" + columnId );
        where.put( "store", "" + storeId );
        updateHandler( transactionHandler, "column_placement", data, where );
    }


    static long addColumnPlacement( XATransactionHandler transactionHandler, int storeId, long columnId, long tableId, PlacementType placementType, String physicalSchemaName, String physicalTableName, String physicalColumnName ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "store", "" + storeId );
        data.put( "column", "" + columnId );
        data.put( "table", "" + tableId );
        data.put( "type", "" + placementType.getId() );
        data.put( "physical_schema", quoteString( physicalSchemaName ) );
        data.put( "physical_table", quoteString( physicalTableName ) );
        data.put( "physical_column", quoteString( physicalColumnName ) );
        return insertHandler( transactionHandler, "column_placement", data );
    }


    static void deleteColumnPlacement( XATransactionHandler transactionHandler, int storeId, long columnId ) throws GenericCatalogException {
        String sql = "DELETE FROM " + quoteIdentifier( "column_placement" ) + " WHERE " + quoteIdentifier( "store" ) + " = " + storeId + " AND " + quoteIdentifier( "column" ) + " = " + columnId;
        try {
            int rowsEffected = transactionHandler.executeUpdate( sql );
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }

    // -------------------------------------------------------------------------------------------------------------------------------------------------------
    //
    //                                                                Keys
    //
    // -------------------------------------------------------------------------------------------------------------------------------------------------------


    private static List<CatalogKey> keyFilter( TransactionHandler transactionHandler, String keyFilter, String keyColumnFilter ) throws GenericCatalogException {
        String keySql = "SELECT k.\"id\", t.\"id\", t.\"name\", s.\"id\", s.\"name\", d.\"id\", d.\"name\" FROM \"key\" k, \"table\" t, \"schema\" s, \"database\" d WHERE k.\"table\" = t.\"id\" AND t.\"schema\" = s.\"id\" AND s.\"database\" = d.\"id\"" + keyFilter + " ORDER BY d.\"name\", s.\"name\", t.\"name\"";
        List<CatalogKey> list = new LinkedList<>();
        try ( ResultSet rs = transactionHandler.executeSelect( keySql ) ) {
            while ( rs.next() ) {
                list.add( new CatalogKey(
                        getLong( rs, 1 ),
                        getLong( rs, 2 ),
                        rs.getString( 3 ),
                        getLong( rs, 4 ),
                        rs.getString( 5 ),
                        getLong( rs, 6 ),
                        rs.getString( 7 )
                ) );
            }
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
        for ( CatalogKey catalogKey : list ) {
            String keyColumnSql = "SELECT c.\"id\", c.\"name\" FROM \"key_column\" kc, \"key\" k, \"column\" c WHERE k.\"id\" = kc.\"key\" AND c.\"id\" = kc.\"column\" AND k.\"id\" = " + catalogKey.id + keyColumnFilter + " ORDER BY k.\"id\", kc.\"seq\"";
            try ( ResultSet rs = transactionHandler.executeSelect( keyColumnSql ) ) {
                List<Long> columnIds = new LinkedList<>();
                List<String> columnNames = new LinkedList<>();
                while ( rs.next() ) {
                    columnIds.add( getLongOrNull( rs, 1 ) );
                    columnNames.add( rs.getString( 2 ) );
                }
                catalogKey.columnIds = columnIds;
                catalogKey.columnNames = columnNames;
            } catch ( SQLException e ) {
                throw new GenericCatalogException( e );
            }
        }
        return list;
    }


    private static List<CatalogForeignKey> foreignKeyFilter( TransactionHandler transactionHandler, String keyFilter ) throws GenericCatalogException {
        String keySql = "SELECT k.\"id\", fk.\"name\", t.\"id\", t.\"name\", s.\"id\", s.\"name\", d.\"id\", d.\"name\", refKey.\"id\", refTab.\"id\", refTab.\"name\", refSch.\"id\", refSch.\"name\", refDat.\"id\", refDat.\"name\", fk.\"on_update\", fk.\"on_delete\" FROM \"key\" k, \"key\" refKey, \"foreign_key\" fk, \"table\" t, \"schema\" s, \"database\" d, \"table\" refTab, \"schema\" refSch, \"database\" refDat WHERE k.\"id\" = fk.\"key\" AND refKey.\"id\" = fk.\"references\" AND t.\"id\" = k.\"table\" AND refTab.\"id\" = refKey.\"table\" AND t.\"schema\" = s.\"id\"  AND s.\"database\" = d.\"id\" AND refTab.\"schema\" = refSch.\"id\" AND refSch.\"database\" = refDat.\"id\"" + keyFilter + " ORDER BY d.\"name\", s.\"name\", t.\"name\", refSch.\"name\", refDat.\"name\", refTab.\"name\"";
        List<CatalogForeignKey> list = new LinkedList<>();
        try ( ResultSet rs = transactionHandler.executeSelect( keySql ) ) {
            while ( rs.next() ) {
                Integer updateRule = getIntOrNull( rs, 16 );
                Integer deleteRule = getIntOrNull( rs, 17 );
                list.add( new CatalogForeignKey(
                        getLong( rs, 1 ),
                        rs.getString( 2 ),
                        getLong( rs, 3 ),
                        rs.getString( 4 ),
                        getLong( rs, 5 ),
                        rs.getString( 6 ),
                        getLong( rs, 7 ),
                        rs.getString( 8 ),
                        getLong( rs, 9 ),
                        getLong( rs, 10 ),
                        rs.getString( 11 ),
                        getLong( rs, 12 ),
                        rs.getString( 13 ),
                        getLong( rs, 14 ),
                        rs.getString( 15 ),
                        updateRule == null ? null : ForeignKeyOption.getById( updateRule ),
                        deleteRule == null ? null : ForeignKeyOption.getById( deleteRule )
                ) );
            }
        } catch ( SQLException | UnknownForeignKeyOptionException e ) {
            throw new GenericCatalogException( e );
        }
        for ( CatalogForeignKey catalogKey : list ) {
            String keyColumnSql = "SELECT c.\"id\", c.\"name\" FROM \"key_column\" kc, \"key\" k, \"column\" c WHERE k.\"id\" = kc.\"key\" AND c.\"id\" = kc.\"column\" AND k.\"id\" = " + catalogKey.id + " ORDER BY k.\"id\", kc.\"seq\"";
            try ( ResultSet rs = transactionHandler.executeSelect( keyColumnSql ) ) {
                List<Long> columnIds = new LinkedList<>();
                List<String> columnNames = new LinkedList<>();
                while ( rs.next() ) {
                    columnIds.add( getLongOrNull( rs, 1 ) );
                    columnNames.add( rs.getString( 2 ) );
                }
                catalogKey.columnIds = columnIds;
                catalogKey.columnNames = columnNames;
            } catch ( SQLException e ) {
                throw new GenericCatalogException( e );
            }
            String refKeyColumnSql = "SELECT c.\"id\", c.\"name\" FROM \"key_column\" kc, \"key\" k, \"column\" c WHERE k.\"id\" = kc.\"key\" AND c.\"id\" = kc.\"column\" AND k.\"id\" = " + catalogKey.referencedKeyId + " ORDER BY k.\"id\", kc.\"seq\"";
            try ( ResultSet rs = transactionHandler.executeSelect( refKeyColumnSql ) ) {
                List<Long> columnIds = new LinkedList<>();
                List<String> columnNames = new LinkedList<>();
                while ( rs.next() ) {
                    columnIds.add( getLongOrNull( rs, 1 ) );
                    columnNames.add( rs.getString( 2 ) );
                }
                catalogKey.referencedKeyColumnIds = columnIds;
                catalogKey.referencedKeyColumnNames = columnNames;
            } catch ( SQLException e ) {
                throw new GenericCatalogException( e );
            }
        }
        return list;
    }


    private static List<CatalogIndex> indexFilter( TransactionHandler transactionHandler, String filter ) throws GenericCatalogException {
        String keySql = "SELECT i.\"id\", i.\"name\", i.\"unique\", i.\"type\", i.\"location\", i.\"key\" FROM \"index\" i WHERE " + filter;
        List<CatalogIndex> list = new LinkedList<>();
        try ( ResultSet rs = transactionHandler.executeSelect( keySql ) ) {
            while ( rs.next() ) {
                list.add( new CatalogIndex(
                        getLong( rs, 1 ),
                        rs.getString( 2 ),
                        rs.getBoolean( 3 ),
                        IndexType.getById( getInt( rs, 4 ) ),
                        getIntOrNull( rs, 5 ),
                        getLong( rs, 6 )
                ) );
            }
        } catch ( SQLException | UnknownIndexTypeException e ) {
            throw new GenericCatalogException( e );
        }
        return list;
    }


    private static List<CatalogConstraint> constraintFilter( TransactionHandler transactionHandler, String filter ) throws GenericCatalogException {
        String keySql = "SELECT co.\"id\", co.\"key\", co.\"type\", co.\"name\" FROM \"constraint\" co WHERE " + filter;
        List<CatalogConstraint> list = new LinkedList<>();
        try ( ResultSet rs = transactionHandler.executeSelect( keySql ) ) {
            while ( rs.next() ) {
                list.add( new CatalogConstraint(
                        getLong( rs, 1 ),
                        getLong( rs, 2 ),
                        ConstraintType.getById( getInt( rs, 3 ) ),
                        rs.getString( 4 )
                ) );
            }
        } catch ( SQLException | UnknownConstraintTypeException e ) {
            throw new GenericCatalogException( e );
        }
        return list;
    }


    static CatalogKey getKey( TransactionHandler transactionHandler, long key ) throws GenericCatalogException, UnknownKeyException {
        String keyFilter = " AND k.\"id\" = " + key;
        String keyColumnFilter = "";
        List<CatalogKey> list = keyFilter( transactionHandler, keyFilter, keyColumnFilter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownKeyException( key );
        }
        return list.get( 0 );
    }


    static CatalogKey getPrimaryKey( TransactionHandler transactionHandler, long key ) throws GenericCatalogException, UnknownKeyException {
        String keyFilter = " AND k.\"id\" = t.\"primary_key\" AND k.\"id\" = " + key;
        String keyColumnFilter = "";
        List<CatalogKey> list = keyFilter( transactionHandler, keyFilter, keyColumnFilter );
        if ( list.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( list.size() == 0 ) {
            throw new UnknownKeyException( key );
        }
        return list.get( 0 );
    }


    static List<CatalogKey> getKeys( XATransactionHandler transactionHandler, long tableId ) throws GenericCatalogException {
        String filter = " AND k.\"table\" = " + tableId;
        return keyFilter( transactionHandler, filter, "" );
    }


    static long addKey( XATransactionHandler transactionHandler, long tableId, List<Long> columnIds ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "table", "" + tableId );
        long keyId = insertHandler( transactionHandler, "key", data );

        int i = 1;
        for ( long columnId : columnIds ) {
            Map<String, String> columnData = new LinkedHashMap<>();
            columnData.put( "key", "" + keyId );
            columnData.put( "column", "" + columnId );
            columnData.put( "seq", "" + i++ );
            insertHandler( transactionHandler, "key_column", columnData );
        }

        return keyId;
    }


    static List<CatalogForeignKey> getForeignKeys( TransactionHandler transactionHandler, long tableId ) throws GenericCatalogException {
        String keyFilter = " AND t.\"id\" = " + tableId;
        return foreignKeyFilter( transactionHandler, keyFilter );
    }


    static List<CatalogForeignKey> getForeignKeysByReference( XATransactionHandler transactionHandler, long referencedKeyId ) throws GenericCatalogException {
        String keyFilter = " AND fk.\"references\" = " + referencedKeyId;
        return foreignKeyFilter( transactionHandler, keyFilter );
    }


    static List<CatalogForeignKey> getExportedKeys( XATransactionHandler transactionHandler, long tableId ) throws GenericCatalogException {
        String keyFilter = " AND refTab.\"id\" = " + tableId;
        return foreignKeyFilter( transactionHandler, keyFilter );
    }


    static CatalogForeignKey getForeignKey( XATransactionHandler transactionHandler, long tableId, String foreignKeyName ) throws GenericCatalogException, UnknownForeignKeyException {
        String keyFilter = " AND t.\"id\" = " + tableId + " AND fk.\"name\" = " + quoteString( foreignKeyName );
        List<CatalogForeignKey> foreignKeys = foreignKeyFilter( transactionHandler, keyFilter );
        if ( foreignKeys.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( foreignKeys.size() == 0 ) {
            throw new UnknownForeignKeyException( foreignKeyName );
        }
        return foreignKeys.get( 0 );
    }


    static CatalogForeignKey getForeignKey( XATransactionHandler transactionHandler, long foreignKeyId ) throws GenericCatalogException, UnknownForeignKeyException {
        String keyFilter = " AND fk.\"key\" = " + foreignKeyId;
        List<CatalogForeignKey> foreignKeys = foreignKeyFilter( transactionHandler, keyFilter );
        if ( foreignKeys.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( foreignKeys.size() == 0 ) {
            throw new UnknownForeignKeyException( foreignKeyId );
        }
        return foreignKeys.get( 0 );
    }


    static long addForeignKey( XATransactionHandler transactionHandler, long keyId, long refKey, String name, ForeignKeyOption onUpdate, ForeignKeyOption onDelete ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "key", "" + keyId );
        data.put( "name", quoteString( name ) );
        data.put( "references", "" + refKey );
        data.put( "on_update", "" + onUpdate.getId() );
        data.put( "on_delete", "" + onDelete.getId() );
        return insertHandler( transactionHandler, "foreign_key", data );
    }


    public static List<CatalogConstraint> getConstraints( XATransactionHandler transactionHandler, long tableId ) throws GenericCatalogException {
        String keyFilter = " AND k.\"table\" = " + tableId;
        List<CatalogKey> keys = keyFilter( transactionHandler, keyFilter, "" );
        List<CatalogConstraint> constraints = new LinkedList<>();
        for ( CatalogKey key : keys ) {
            String constraintFilter = " co.\"key\" = " + key.id;
            List<CatalogConstraint> constraintsOfKey = constraintFilter( transactionHandler, constraintFilter );
            constraintsOfKey.forEach( co -> co.key = key );
            constraints.addAll( constraintsOfKey );
        }
        return constraints;
    }


    static List<CatalogConstraint> getConstraintsByKey( XATransactionHandler transactionHandler, long keyId ) throws GenericCatalogException {
        String keyFilter = " AND k.\"id\" = " + keyId;
        List<CatalogKey> keys = keyFilter( transactionHandler, keyFilter, "" );
        List<CatalogConstraint> constraints = new LinkedList<>();
        for ( CatalogKey key : keys ) {
            String constraintFilter = " co.\"key\" = " + key.id;
            List<CatalogConstraint> constraintsOfKey = constraintFilter( transactionHandler, constraintFilter );
            constraintsOfKey.forEach( co -> co.key = key );
            constraints.addAll( constraintsOfKey );
        }
        return constraints;
    }


    static CatalogConstraint getConstraint( XATransactionHandler transactionHandler, long constraintId ) throws GenericCatalogException, UnknownKeyException, UnknownConstraintException {
        String constraintFilter = " co.\"id\" = " + constraintId;
        List<CatalogConstraint> constraints = constraintFilter( transactionHandler, constraintFilter );
        if ( constraints.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( constraints.size() == 0 ) {
            throw new UnknownConstraintException( constraintId );
        }
        CatalogConstraint constraint = constraints.get( 0 );
        constraint.key = getKey( transactionHandler, constraint.keyId );
        return constraint;
    }


    static CatalogConstraint getConstraint( XATransactionHandler transactionHandler, long tableId, String constraintName ) throws GenericCatalogException, UnknownConstraintException {
        String keyFilter = " AND k.\"table\" = " + tableId;
        List<CatalogKey> keys = keyFilter( transactionHandler, keyFilter, "" );
        List<CatalogConstraint> constraints = new LinkedList<>();
        for ( CatalogKey key : keys ) {
            String constraintFilter = " co.\"key\" = " + key.id + " AND co.\"name\" = " + quoteString( constraintName );
            List<CatalogConstraint> constraintOfKey = constraintFilter( transactionHandler, constraintFilter );
            constraintOfKey.forEach( i -> i.key = key );
            constraints.addAll( constraintOfKey );
        }
        if ( constraints.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( constraints.size() == 0 ) {
            throw new UnknownConstraintException( indexName );
        }
        return constraints.get( 0 );
    }


    static long addConstraint( XATransactionHandler transactionHandler, long keyId, ConstraintType type, String constraintName ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "key", "" + keyId );
        data.put( "type", "" + type.getId() );
        data.put( "name", quoteString( constraintName ) );
        return insertHandler( transactionHandler, "constraint", data );
    }


    static List<CatalogIndex> getIndexes( XATransactionHandler transactionHandler, long tableId, boolean onlyUnique ) throws GenericCatalogException {
        String keyFilter = " AND k.\"table\" = " + tableId;
        List<CatalogKey> keys = keyFilter( transactionHandler, keyFilter, "" );
        List<CatalogIndex> indexes = new LinkedList<>();
        for ( CatalogKey key : keys ) {
            String indexFilter = " i.\"key\" = " + key.id;
            if ( onlyUnique ) {
                indexFilter += " AND i.\"unique\" = true";
            }
            List<CatalogIndex> indexesOfKey = indexFilter( transactionHandler, indexFilter );
            indexesOfKey.forEach( i -> i.key = key );
            indexes.addAll( indexesOfKey );
        }
        return indexes;
    }


    static CatalogIndex getIndex( XATransactionHandler transactionHandler, long tableId, String indexName ) throws GenericCatalogException, UnknownIndexException {
        String keyFilter = " AND k.\"table\" = " + tableId;
        List<CatalogKey> keys = keyFilter( transactionHandler, keyFilter, "" );
        List<CatalogIndex> indexes = new LinkedList<>();
        for ( CatalogKey key : keys ) {
            String indexFilter = " i.\"key\" = " + key.id + " AND i.\"name\" = " + quoteString( indexName );
            List<CatalogIndex> indexesOfKey = indexFilter( transactionHandler, indexFilter );
            indexesOfKey.forEach( i -> i.key = key );
            indexes.addAll( indexesOfKey );
        }
        if ( indexes.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( indexes.size() == 0 ) {
            throw new UnknownIndexException( indexName );
        }
        return indexes.get( 0 );
    }


    static CatalogIndex getIndex( XATransactionHandler transactionHandler, long indexId ) throws GenericCatalogException, UnknownIndexException {
        String indexFilter = " i.\"id\" = " + indexId;
        List<CatalogIndex> indexes = indexFilter( transactionHandler, indexFilter );
        if ( indexes.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( indexes.size() == 0 ) {
            throw new UnknownIndexException( indexId );
        }
        CatalogIndex index = indexes.get( 0 );
        // Get corresponding key
        String keyFilter = " AND k.\"id\" = " + index.keyId;
        List<CatalogKey> keys = keyFilter( transactionHandler, keyFilter, "" );
        if ( indexes.size() > 1 ) {
            throw new GenericCatalogException( "More than one result. This combination of parameters should be unique. But it seams, it is not..." );
        } else if ( indexes.size() == 0 ) {
            throw new UnknownIndexException( indexId );
        }
        index.key = keys.get( 0 );
        return index;
    }


    static List<CatalogIndex> getIndexesByKey( XATransactionHandler transactionHandler, long keyId ) throws GenericCatalogException {
        String keyFilter = " AND k.\"id\" = " + keyId;
        List<CatalogKey> keys = keyFilter( transactionHandler, keyFilter, "" );
        List<CatalogIndex> indexes = new LinkedList<>();
        for ( CatalogKey key : keys ) {
            String indexFilter = " i.\"key\" = " + key.id;
            List<CatalogIndex> indexesOfKey = indexFilter( transactionHandler, indexFilter );
            indexesOfKey.forEach( i -> i.key = key );
            indexes.addAll( indexesOfKey );
        }
        return indexes;
    }


    static long addIndex( XATransactionHandler transactionHandler, long keyId, IndexType type, boolean unique, Long location, String indexName ) throws GenericCatalogException {
        Map<String, String> data = new LinkedHashMap<>();
        data.put( "key", "" + keyId );
        data.put( "type", "" + type.getId() );
        data.put( "unique", "" + unique );
        data.put( "location", "" + location );
        data.put( "name", quoteString( indexName ) );
        return insertHandler( transactionHandler, "index", data );
    }


    static void deleteIndex( XATransactionHandler transactionHandler, long indexId ) throws GenericCatalogException {
        String sql = "DELETE FROM " + quoteIdentifier( "index" ) + " WHERE " + quoteIdentifier( "id" ) + " = " + indexId;
        try {
            int rowsEffected = transactionHandler.executeUpdate( sql );
            if ( rowsEffected != 1 ) {
                throw new GenericCatalogException( "Expected only one effected row, but " + rowsEffected + " have been effected." );
            }
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }


    static void deleteKey( XATransactionHandler transactionHandler, long keyId ) throws GenericCatalogException {
        // Delete key columns
        String sql = "DELETE FROM " + quoteIdentifier( "key_column" ) + " WHERE " + quoteIdentifier( "key" ) + " = " + keyId;
        try {
            transactionHandler.executeUpdate( sql );
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
        // Delete key
        sql = "DELETE FROM " + quoteIdentifier( "key" ) + " WHERE " + quoteIdentifier( "id" ) + " = " + keyId;
        try {
            int rowsEffected = transactionHandler.executeUpdate( sql );
            if ( rowsEffected != 1 ) {
                throw new GenericCatalogException( "Expected only one effected row, but " + rowsEffected + " have been effected." );
            }
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }


    static void deleteForeignKey( XATransactionHandler transactionHandler, long keyId ) throws GenericCatalogException {
        String sql = "DELETE FROM " + quoteIdentifier( "foreign_key" ) + " WHERE " + quoteIdentifier( "key" ) + " = " + keyId;
        try {
            int rowsEffected = transactionHandler.executeUpdate( sql );
            if ( rowsEffected != 1 ) {
                throw new GenericCatalogException( "Expected only one effected row, but " + rowsEffected + " have been effected." );
            }
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }


    static void deleteConstraint( XATransactionHandler transactionHandler, long constraintId ) throws GenericCatalogException {
        String sql = "DELETE FROM " + quoteIdentifier( "constraint" ) + " WHERE " + quoteIdentifier( "id" ) + " = " + constraintId;
        try {
            int rowsEffected = transactionHandler.executeUpdate( sql );
            if ( rowsEffected != 1 ) {
                throw new GenericCatalogException( "Expected only one effected row, but " + rowsEffected + " have been effected." );
            }
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }


    // -------------------------------------------------------------------------------------------------------------------------------------------------------
    //
    //                                                                   Helpers
    //
    // -------------------------------------------------------------------------------------------------------------------------------------------------------


    private static long insertHandler( XATransactionHandler transactionHandler, String tableName, Map<String, String> data ) throws GenericCatalogException {
        StringBuilder builder = new StringBuilder();
        builder.append( "INSERT INTO " ).append( quoteIdentifier( tableName ) ).append( " ( " );
        boolean first = true;
        for ( String columnName : data.keySet() ) {
            if ( !first ) {
                builder.append( ", " );
            }
            first = false;
            builder.append( quoteIdentifier( columnName ) );
        }
        builder.append( " ) VALUES ( " );
        first = true;
        for ( String value : data.values() ) {
            if ( !first ) {
                builder.append( ", " );
            }
            first = false;
            builder.append( value );
        }
        builder.append( " );" );

        try {
            int rowCount = transactionHandler.executeUpdate( builder.toString() );
            if ( rowCount != 1 ) {
                throw new GenericCatalogException( "Expected row count of one but got " + rowCount );
            }
            // Get Schema Id (auto increment)
            ResultSet rs = transactionHandler.getGeneratedKeys();
            if ( rs.next() ) {
                return getLong( rs, 1 );
            } else {
                throw new GenericCatalogException( "Something went wrong. Unable to retrieve inserted schema id." );
            }
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }


    private static void updateHandler( XATransactionHandler transactionHandler, String tableName, Map<String, String> data, Map<String, String> where ) throws GenericCatalogException {
        StringBuilder builder = new StringBuilder();
        builder.append( "UPDATE " ).append( quoteIdentifier( tableName ) ).append( " SET " );
        boolean first = true;
        for ( Map.Entry<String, String> entry : data.entrySet() ) {
            if ( !first ) {
                builder.append( ", " );
            }
            first = false;
            builder.append( quoteIdentifier( entry.getKey() ) ).append( "=" ).append( entry.getValue() );
        }
        first = true;
        for ( Map.Entry<String, String> entry : where.entrySet() ) {
            if ( first ) {
                builder.append( " WHERE " );
                first = false;
            } else {
                builder.append( " AND " );
            }
            builder.append( quoteIdentifier( entry.getKey() ) ).append( "=" ).append( entry.getValue() );
        }
        try {
            int rowCount = transactionHandler.executeUpdate( builder.toString() );
            if ( rowCount != 1 ) {
                throw new GenericCatalogException( "Expected row count of one but got " + rowCount );
            }
        } catch ( SQLException e ) {
            throw new GenericCatalogException( e );
        }
    }


    private static String quoteIdentifier( String identifier ) {
        return "\"" + identifier + "\"";
    }


    private static String quoteString( String s ) {
        if ( s == null ) {
            return null;
        } else {
            return "'" + s + "'";
        }
    }


    private static int getInt( ResultSet rs, int index ) throws SQLException {
        int v = rs.getInt( index );
        if ( rs.wasNull() ) {
            throw new NullPointerException( "Not expecting null here!" );
        }
        return v;
    }


    private static Integer getIntOrNull( ResultSet rs, int index ) throws SQLException {
        int v = rs.getInt( index );
        if ( rs.wasNull() ) {
            return null;
        }
        return v;
    }


    private static long getLong( ResultSet rs, int index ) throws SQLException {
        long v = rs.getLong( index );
        if ( rs.wasNull() ) {
            throw new NullPointerException( "Not expecting null here!" );
        }
        return v;
    }


    private static Long getLongOrNull( ResultSet rs, int index ) throws SQLException {
        long v = rs.getLong( index );
        if ( rs.wasNull() ) {
            return null;
        }
        return v;
    }

}
