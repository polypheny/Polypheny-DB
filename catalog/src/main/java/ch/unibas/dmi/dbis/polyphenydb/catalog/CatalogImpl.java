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
import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.UnknownTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDataPlacement;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogForeignKey;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogIndex;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogKey;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogPrimaryKey;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogStore;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogTransactionException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownColumnException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownEncodingException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownKeyException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownStoreException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
import java.util.LinkedList;
import java.util.List;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CatalogImpl extends Catalog {

    private static final Logger LOG = LoggerFactory.getLogger( CatalogManagerImpl.class );


    CatalogImpl( PolyXid xid ) {
        super( xid );
    }


    /**
     * Get all databases
     *
     * @param pattern A pattern for the database name
     * @return List of databases
     */
    @Override
    public List<CatalogDatabase> getDatabases( Pattern pattern ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getDatabases( transactionHandler, pattern );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownCollationException | UnknownEncodingException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the database with the given name.
     *
     * @param databaseName The name of the database
     * @return The database
     * @throws UnknownDatabaseException If there is no database with this name.
     */
    @Override
    public CatalogDatabase getDatabase( String databaseName ) throws GenericCatalogException, UnknownDatabaseException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getDatabase( transactionHandler, databaseName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the database with the given name.
     *
     * @param databaseId The id of the database
     * @return The database
     * @throws UnknownDatabaseException If there is no database with this name.
     */
    @Override
    public CatalogDatabase getDatabase( long databaseId ) throws GenericCatalogException, UnknownDatabaseException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getDatabase( transactionHandler, databaseId );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all schemas which fit to the specified filter pattern.
     * <code>getSchemas(xid, null, null)</code> returns all schemas of all databases.
     *
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogSchema> getSchemas( Pattern databaseNamePattern, Pattern schemaNamePattern ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getSchemas( transactionHandler, databaseNamePattern, schemaNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownSchemaTypeException | UnknownCollationException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all schemas of the specified database which fit to the specified filter pattern.
     * <code>getSchemas(xid, databaseName, null)</code> returns all schemas of the database.
     *
     * @param databaseId The id of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogSchema> getSchemas( long databaseId, Pattern schemaNamePattern ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getSchemas( transactionHandler, databaseId, schemaNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownSchemaTypeException | UnknownCollationException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the schema with the given name in the specified database.
     *
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @return The schema
     * @throws UnknownSchemaException If there is no schema with this name in the specified database.
     */
    @Override
    public CatalogSchema getSchema( String databaseName, String schemaName ) throws GenericCatalogException, UnknownSchemaException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getSchema( transactionHandler, databaseName, schemaName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownSchemaTypeException | UnknownCollationException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the schema with the given name in the specified database.
     *
     * @param databaseId The id of the database
     * @param schemaName The name of the schema
     * @return The schema
     * @throws UnknownSchemaException If there is no schema with this name in the specified database.
     */
    @Override
    public CatalogSchema getSchema( long databaseId, String schemaName ) throws GenericCatalogException, UnknownSchemaException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getSchema( transactionHandler, databaseId, schemaName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownSchemaTypeException | UnknownCollationException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Adds a schema in a specified database
     *
     * @param name The name of the schema
     * @param databaseId The id of the associated database
     * @param ownerId The owner of this schema
     * @param encoding The default encoding of the schema
     * @param collation The default collation of the schema
     * @param schemaType The type of this schema
     * @return The id of the inserted schema
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public long addSchema( String name, long databaseId, int ownerId, Encoding encoding, Collation collation, SchemaType schemaType ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogDatabase database = Statements.getDatabase( transactionHandler, databaseId );
            CatalogUser owner = Statements.getUser( transactionHandler, ownerId );
            return Statements.addSchema( transactionHandler, name, database.id, owner.id, encoding, collation, schemaType );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | UnknownDatabaseException | GenericCatalogException | UnknownUserException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Checks weather a schema with the specified name exists in a database.
     *
     * @param databaseId The if of the database
     * @param schemaName The name of the schema to check
     * @return True if there is a schema with this name. False if not.
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public boolean checkIfExistsSchema( long databaseId, String schemaName ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogDatabase database = Statements.getDatabase( transactionHandler, databaseId );
            Statements.getSchema( transactionHandler, database.id, schemaName );
            return true;
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownSchemaTypeException | UnknownCollationException | UnknownDatabaseException | GenericCatalogException e ) {
            throw new GenericCatalogException( e );
        } catch ( UnknownSchemaException e ) {
            return false;
        }
    }


    /**
     * Delete a schema from the catalog
     *
     * @param schemaId The if of the schema to delete
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public void deleteSchema( long schemaId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteSchema( transactionHandler, schemaId );
        } catch ( CatalogConnectionException | CatalogTransactionException | GenericCatalogException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all tables of the specified schema which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param schemaId The id of the schema
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogTable> getTables( long schemaId, Pattern tableNamePattern ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTables( transactionHandler, schemaId, tableNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all tables of the specified database which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param databaseId The id of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogTable> getTables( long databaseId, Pattern schemaNamePattern, Pattern tableNamePattern ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTables( transactionHandler, databaseId, schemaNamePattern, tableNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all tables of the specified database which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogTable> getTables( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTables( transactionHandler, databaseNamePattern, schemaNamePattern, tableNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the table with the given name in the specified schema.
     *
     * @param schemaId The id of the schema
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    @Override
    public CatalogTable getTable( long schemaId, String tableName ) throws UnknownTableException, GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTable( transactionHandler, schemaId, tableName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the table with the given name in the specified database and schema.
     *
     * @param databaseId The id of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    @Override
    public CatalogTable getTable( long databaseId, String schemaName, String tableName ) throws UnknownTableException, GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTable( transactionHandler, databaseId, schemaName, tableName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the table with the given name in the specified database and schema.
     *
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    @Override
    public CatalogTable getTable( String databaseName, String schemaName, String tableName ) throws UnknownTableException, GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTable( transactionHandler, databaseName, schemaName, tableName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Adds a table to a specified schema.
     *
     * @param name The name of the table to add
     * @param schemaId The id of the schema
     * @param ownerId The if of the owner
     * @param encoding The default encoding of this table
     * @param collation The default collation of this table
     * @param tableType The table type
     * @param definition The definition of this table (e.g. a SQL string; null if not applicable)
     * @return The id of the inserted table
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public long addTable( String name, long schemaId, int ownerId, Encoding encoding, Collation collation, TableType tableType, String definition ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogSchema schema = Statements.getSchema( transactionHandler, schemaId );
            CatalogUser owner = Statements.getUser( transactionHandler, ownerId );
            return Statements.addTable( transactionHandler, name, schema.id, owner.id, encoding, collation, tableType, definition );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | GenericCatalogException | UnknownUserException | UnknownSchemaTypeException | UnknownSchemaException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Delete the specified table. Columns, Keys and Data Placements need to be deleted before.
     *
     * @param tableId The id of the table to delete
     */
    @Override
    public void deleteTable( long tableId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteTable( transactionHandler, tableId );
        } catch ( CatalogConnectionException | GenericCatalogException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Set the primary key of a table
     *
     * @param tableId The id of the table
     * @param keyId The id of the key to set as primary key. Set null to set no primary key.
     */
    @Override
    public void setPrimaryKey( long tableId, Long keyId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.setPrimaryKey( transactionHandler, tableId, keyId );
        } catch ( CatalogConnectionException | GenericCatalogException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Adds a placement for a table
     *
     * @param storeId The store on which the table should be placed on
     * @param tableId The id of the table to be placed
     * @throws GenericCatalogException A generic catalog exception
     */
    @Override
    public void addDataPlacement( int storeId, long tableId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogStore store = Statements.getStore( transactionHandler, storeId );
            CatalogTable table = Statements.getTable( transactionHandler, tableId );
            Statements.addDataPlacement( transactionHandler, store.id, table.id );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | GenericCatalogException | UnknownStoreException | UnknownTableTypeException | UnknownTableException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Deletes a data placement
     *
     * @param storeId The id of the store
     * @param tableId The id of the table
     */
    @Override
    public void deleteDataPlacement( int storeId, long tableId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteDataPlacement( transactionHandler, storeId, tableId );
        } catch ( CatalogConnectionException | GenericCatalogException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all columns of the specified table.
     *
     * @param tableId The id of the table
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogColumn> getColumns( long tableId ) throws GenericCatalogException, UnknownCollationException, UnknownEncodingException, UnknownTypeException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumns( transactionHandler, tableId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all columns of the specified database which fit to the specified filter patterns.
     * <code>getColumns(xid, databaseName, null, null, null)</code> returns all columns of the database.
     *
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @param columnNamePattern Pattern for the column name. null returns all.
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogColumn> getColumns( Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern, Pattern columnNamePattern ) throws GenericCatalogException, UnknownCollationException, UnknownEncodingException, UnknownColumnException, UnknownTypeException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumns( transactionHandler, databaseNamePattern, schemaNamePattern, tableNamePattern, columnNamePattern );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param tableId The id of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     * @throws UnknownColumnException If there is no column with this name in the specified table of the database and schema.
     */
    @Override
    public CatalogColumn getColumn( long tableId, String columnName ) throws GenericCatalogException, UnknownColumnException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumn( transactionHandler, tableId, columnName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | UnknownTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     * @throws UnknownColumnException If there is no column with this name in the specified table of the database and schema.
     */
    @Override
    public CatalogColumn getColumn( String databaseName, String schemaName, String tableName, String columnName ) throws GenericCatalogException, UnknownColumnException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumn( transactionHandler, databaseName, schemaName, tableName, columnName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | UnknownTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Adds a column.
     *
     * @param name The name of the column
     * @param tableId The id of the corresponding table
     * @param position The ordinal position of the column (starting with 1)
     * @param type The type of the column
     * @param length The length of the field (if applicable, else null)
     * @param precision The precision of the field (if applicable, else null)
     * @param nullable Weather the column can contain null values
     * @param encoding The encoding of the field (if applicable, else null)
     * @param collation The collation of the field (if applicable, else null)
     * @param forceDefault Weather to force the default value
     * @return The id of the inserted column
     */
    @Override
    public long addColumn( String name, long tableId, int position, PolySqlType type, Integer length, Integer precision, boolean nullable, Encoding encoding, Collation collation, boolean forceDefault ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogTable table = Statements.getTable( transactionHandler, tableId );
            return Statements.addColumn( transactionHandler, name, table.id, position, type, length, precision, nullable, encoding, collation, forceDefault );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | GenericCatalogException | UnknownTableTypeException | UnknownTableException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Delete the specified column. A potential default value has to be delete before.
     *
     * @param columnId The id of the column to delete
     */
    @Override
    public void deleteColumn( long columnId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteColumn( transactionHandler, columnId );
        } catch ( CatalogConnectionException | GenericCatalogException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns a specified key
     *
     * @param key The id of the key
     * @return The key
     */
    @Override
    public CatalogKey getKey( long key ) throws GenericCatalogException, UnknownKeyException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getKey( transactionHandler, key );
        } catch ( CatalogConnectionException | GenericCatalogException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns all keys of a table
     *
     * @param tableId The id of the key
     * @return List of keys
     */
    @Override
    public List<CatalogKey> getKeys( long tableId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getKeys( transactionHandler, tableId );
        } catch ( CatalogConnectionException | GenericCatalogException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns a specified primary key
     *
     * @param key The id of the primary key
     * @return The primary key
     */
    @Override
    public CatalogPrimaryKey getPrimaryKey( long key ) throws GenericCatalogException, UnknownKeyException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return new CatalogPrimaryKey( Statements.getKey( transactionHandler, key ) );
        } catch ( CatalogConnectionException | GenericCatalogException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns all (imported) foreign keys of a specified table
     *
     * @param tableId The id of the table
     * @return List of foreign keys
     */
    @Override
    public List<CatalogForeignKey> getForeignKeys( long tableId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getForeignKeys( transactionHandler, tableId );
        } catch ( CatalogConnectionException | GenericCatalogException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns all foreign keys that reference the specified table (exported keys).
     *
     * @param tableId The id of the table
     * @return List of foreign keys
     */
    @Override
    public List<CatalogForeignKey> getExportedKeys( long tableId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getExportedKeys( transactionHandler, tableId );
        } catch ( CatalogConnectionException | GenericCatalogException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns all indexes of a table
     *
     * @param tableId The id of the table
     * @param onlyUnique true if only indexes for unique values are returned. false if all indexes are returned.
     * @return List of indexes
     */
    @Override
    public List<CatalogIndex> getIndexes( long tableId, boolean onlyUnique ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getIndexes( transactionHandler, tableId, onlyUnique );
        } catch ( CatalogConnectionException | GenericCatalogException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Delete the specified index
     *
     * @param indexId The id of the index to drop
     */
    @Override
    public void deleteIndex( long indexId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteIndex( transactionHandler, indexId );
        } catch ( CatalogConnectionException | GenericCatalogException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Delete the specified key
     *
     * @param keyId The id of the key to drop
     */
    @Override
    public void deleteKey( long keyId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteKey( transactionHandler, keyId );
        } catch ( CatalogConnectionException | GenericCatalogException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Delete the specified foreign key (deletes the corresponding key but does not delete the referenced key). If there is an index on this key, make sure to delete it before.
     *
     * @param keyId The id of the key to drop
     */
    @Override
    public void deleteForeignKey( long keyId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            Statements.deleteForeignKey( transactionHandler, keyId );
            Statements.deleteKey( transactionHandler, keyId );
        } catch ( CatalogConnectionException | GenericCatalogException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public CatalogCombinedDatabase getCombinedDatabase( long databaseId ) throws GenericCatalogException, UnknownSchemaException, UnknownTableException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return getCombinedDatabase( transactionHandler, databaseId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }

    }


    private CatalogCombinedDatabase getCombinedDatabase( XATransactionHandler transactionHandler, long databaseId ) throws GenericCatalogException, UnknownSchemaException, UnknownTableException {
        try {
            CatalogDatabase database = Statements.getDatabase( transactionHandler, databaseId );
            List<CatalogSchema> schemas = Statements.getSchemas( transactionHandler, databaseId, null );
            List<CatalogCombinedSchema> combinedSchemas = new LinkedList<>();
            for ( CatalogSchema schema : schemas ) {
                combinedSchemas.add( getCombinedSchema( transactionHandler, schema.id ) );
            }
            CatalogSchema defaultSchema = null;
            if ( database.defaultSchemaId != null ) {
                defaultSchema = Statements.getSchema( transactionHandler, database.defaultSchemaId );
            }
            CatalogUser owner = Statements.getUser( transactionHandler, database.ownerId );
            return new CatalogCombinedDatabase( database, combinedSchemas, defaultSchema, owner );
        } catch ( UnknownEncodingException | UnknownCollationException | GenericCatalogException | UnknownSchemaTypeException | UnknownDatabaseException | UnknownUserException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public CatalogCombinedSchema getCombinedSchema( long schemaId ) throws GenericCatalogException, UnknownSchemaException, UnknownTableException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return getCombinedSchema( transactionHandler, schemaId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }

    }


    private CatalogCombinedSchema getCombinedSchema( XATransactionHandler transactionHandler, long schemaId ) throws GenericCatalogException, UnknownSchemaException, UnknownTableException {
        try {
            CatalogSchema schema = Statements.getSchema( transactionHandler, schemaId );
            List<CatalogTable> tables = Statements.getTables( transactionHandler, schemaId, null );
            List<CatalogCombinedTable> combinedTables = new LinkedList<>();
            for ( CatalogTable table : tables ) {
                combinedTables.add( getCombinedTable( transactionHandler, table.id ) );
            }
            CatalogDatabase database = Statements.getDatabase( transactionHandler, schema.databaseId );
            CatalogUser owner = Statements.getUser( transactionHandler, schema.ownerId );
            return new CatalogCombinedSchema( schema, combinedTables, database, owner );
        } catch ( UnknownEncodingException | UnknownCollationException | GenericCatalogException | UnknownTableTypeException | UnknownSchemaTypeException | UnknownDatabaseException | UnknownUserException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public CatalogCombinedTable getCombinedTable( long tableId ) throws GenericCatalogException, UnknownTableException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return getCombinedTable( transactionHandler, tableId );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    private CatalogCombinedTable getCombinedTable( XATransactionHandler transactionHandler, long tableId ) throws GenericCatalogException, UnknownTableException {
        try {
            CatalogTable table = Statements.getTable( transactionHandler, tableId );
            List<CatalogColumn> columns = Statements.getColumns( transactionHandler, tableId );
            CatalogSchema schema = Statements.getSchema( transactionHandler, table.schemaId );
            CatalogDatabase database = Statements.getDatabase( transactionHandler, schema.databaseId );
            CatalogUser owner = Statements.getUser( transactionHandler, table.ownerId );
            List<CatalogDataPlacement> placements = Statements.getDataPlacements( transactionHandler, tableId );
            List<CatalogKey> keys = Statements.getKeys( transactionHandler, tableId );
            return new CatalogCombinedTable( table, columns, schema, database, owner, placements, keys );
        } catch ( UnknownEncodingException | UnknownCollationException | UnknownTypeException | GenericCatalogException | UnknownTableTypeException | UnknownSchemaTypeException | UnknownSchemaException | UnknownDatabaseException | UnknownUserException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public boolean prepare() throws CatalogTransactionException {
        val transactionHandler = XATransactionHandler.getTransactionHandler( xid );
        if ( XATransactionHandler.hasTransactionHandler( xid ) ) {
            return transactionHandler.prepare();
        } else {
            // e.g. SELECT 1; commit;
            LOG.debug( "Unknown transaction handler. This is not necessarily a problem as long as the query has not initiated any catalog lookups." );
            return true;
        }
    }


    @Override
    public void commit() throws CatalogTransactionException {
        val transactionHandler = XATransactionHandler.getTransactionHandler( xid );
        if ( XATransactionHandler.hasTransactionHandler( xid ) ) {
            transactionHandler.commit();
        } else {
            // e.g. SELECT 1; commit;
            LOG.debug( "Unknown transaction handler. This is not necessarily a problem as long as the query has not initiated any catalog lookups." );
        }
        CatalogManagerImpl.getInstance().removeCatalog( xid );
    }


    @Override
    public void rollback() throws CatalogTransactionException {
        val transactionHandler = XATransactionHandler.getTransactionHandler( xid );
        if ( XATransactionHandler.hasTransactionHandler( xid ) ) {
            transactionHandler.rollback();
        } else {
            // e.g. SELECT 1; commit;
            LOG.debug( "Unknown transaction handler. This is not necessarily a problem as long as the query has not initiated any catalog lookups." );
        }
        CatalogManagerImpl.getInstance().removeCatalog( xid );
    }


}
