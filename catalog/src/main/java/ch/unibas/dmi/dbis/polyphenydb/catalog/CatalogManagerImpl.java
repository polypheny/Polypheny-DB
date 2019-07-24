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


/**
 *
 */
public class CatalogManagerImpl extends CatalogManager {

    private static final Logger LOG = LoggerFactory.getLogger( CatalogManagerImpl.class );

    private static final boolean CREATE_SCHEMA = true;

    private static final CatalogManagerImpl INSTANCE = new CatalogManagerImpl();


    @SuppressWarnings("WeakerAccess")
    public static CatalogManagerImpl getInstance() {
        return INSTANCE;
    }


    private CatalogManagerImpl() {
        if ( CREATE_SCHEMA ) {
            LocalTransactionHandler transactionHandler = null;
            try {
                transactionHandler = LocalTransactionHandler.getTransactionHandler();
                Statements.dropSchema( transactionHandler );
                Statements.createSchema( transactionHandler );
                transactionHandler.commit();
            } catch ( CatalogConnectionException | CatalogTransactionException e ) {
                LOG.error( "Exception while creating catalog schema", e );
                try {
                    if ( transactionHandler != null ) {
                        transactionHandler.rollback();
                    }
                } catch ( CatalogTransactionException e1 ) {
                    LOG.error( "Exception while rollback", e );
                }
            }
        }
    }



    /**
     * Get all databases
     *
     * @param xid The transaction identifier
     * @param pattern A pattern for the database name
     * @return List of databases
     */
    @Override
    public List<CatalogDatabase> getDatabases( PolyXid xid, Pattern pattern ) throws GenericCatalogException {
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
     * @param xid The transaction identifier
     * @param databaseName The name of the database
     * @return The database
     * @throws UnknownDatabaseException If there is no database with this name.
     */
    @Override
    public CatalogDatabase getDatabase( PolyXid xid, String databaseName ) throws GenericCatalogException, UnknownDatabaseException {
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
     * @param xid The transaction identifier
     * @param databaseId The id of the database
     * @return The database
     * @throws UnknownDatabaseException If there is no database with this name.
     */
    @Override
    public CatalogDatabase getDatabase( PolyXid xid, long databaseId ) throws GenericCatalogException, UnknownDatabaseException {
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
     * @param xid The transaction identifier
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogSchema> getSchemas( PolyXid xid, Pattern databaseNamePattern, Pattern schemaNamePattern ) throws GenericCatalogException {
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
     * @param xid The transaction identifier
     * @param databaseId The id of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogSchema> getSchemas( PolyXid xid, long databaseId, Pattern schemaNamePattern ) throws GenericCatalogException {
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
     * @param xid The transaction identifier
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @return The schema
     * @throws UnknownSchemaException If there is no schema with this name in the specified database.
     */
    @Override
    public CatalogSchema getSchema( PolyXid xid, String databaseName, String schemaName ) throws GenericCatalogException, UnknownSchemaException {
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
     * @param xid The transaction identifier
     * @param databaseId The id of the database
     * @param schemaName The name of the schema
     * @return The schema
     * @throws UnknownSchemaException If there is no schema with this name in the specified database.
     */
    @Override
    public CatalogSchema getSchema( PolyXid xid, long databaseId, String schemaName ) throws GenericCatalogException, UnknownSchemaException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getSchema( transactionHandler, databaseId, schemaName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownSchemaTypeException | UnknownCollationException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public long addSchema( PolyXid xid, String name, long databaseId, int ownerId, Encoding encoding, Collation collation, SchemaType schemaType ) throws GenericCatalogException {
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
     * Get all tables of the specified schema which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param xid The transaction identifier
     * @param schemaId The id of the schema
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogTable> getTables( PolyXid xid, long schemaId, Pattern tableNamePattern ) throws GenericCatalogException {
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
     * @param xid The transaction identifier
     * @param databaseId The id of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogTable> getTables( PolyXid xid, long databaseId, Pattern schemaNamePattern, Pattern tableNamePattern ) throws GenericCatalogException {
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
     * @param xid The transaction identifier
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogTable> getTables( PolyXid xid, Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern ) throws GenericCatalogException {
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
     * @param xid The transaction identifier
     * @param schemaId The id of the schema
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    @Override
    public CatalogTable getTable( PolyXid xid, long schemaId, String tableName ) throws UnknownTableException, GenericCatalogException {
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
     * @param xid The transaction identifier
     * @param databaseId The id of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    @Override
    public CatalogTable getTable( PolyXid xid, long databaseId, String schemaName, String tableName ) throws UnknownTableException, GenericCatalogException {
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
     * @param xid The transaction identifier
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @return The table
     * @throws UnknownTableException If there is no table with this name in the specified database and schema.
     */
    @Override
    public CatalogTable getTable( PolyXid xid, String databaseName, String schemaName, String tableName ) throws UnknownTableException, GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getTable( transactionHandler, databaseName, schemaName, tableName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public long addTable( PolyXid xid, String name, long schemaId, int ownerId, Encoding encoding, Collation collation, TableType tableType, String definition ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogSchema schema = Statements.getSchema( transactionHandler, schemaId );
            CatalogUser owner = Statements.getUser( transactionHandler, ownerId );
            return Statements.addTable( transactionHandler, name, schema.id, owner.id, encoding, collation, tableType, definition );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | GenericCatalogException | UnknownUserException | UnknownSchemaTypeException | UnknownSchemaException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public long addDataPlacement( PolyXid xid, int storeId, long tableId ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogStore store = Statements.getStore( transactionHandler, storeId );
            CatalogTable table = Statements.getTable( transactionHandler, tableId );
            return Statements.addDataPlacement( transactionHandler, store.id, table.id );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | GenericCatalogException | UnknownStoreException | UnknownTableTypeException | UnknownTableException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all columns of the specified table.
     *
     * @param xid The transaction identifier
     * @param tableId The id of the table
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogColumn> getColumns( PolyXid xid, long tableId ) throws GenericCatalogException, UnknownCollationException, UnknownEncodingException, UnknownTypeException {
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
     * @param xid The transaction identifier
     * @param databaseNamePattern Pattern for the database name. null returns all.
     * @param schemaNamePattern Pattern for the schema name. null returns all.
     * @param tableNamePattern Pattern for the table name. null returns all.
     * @param columnNamePattern Pattern for the column name. null returns all.
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogColumn> getColumns( PolyXid xid, Pattern databaseNamePattern, Pattern schemaNamePattern, Pattern tableNamePattern, Pattern columnNamePattern ) throws GenericCatalogException, UnknownCollationException, UnknownEncodingException, UnknownColumnException, UnknownTypeException {
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
     * @param xid The transaction identifier
     * @param tableId The id of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     * @throws UnknownColumnException If there is no column with this name in the specified table of the database and schema.
     */
    @Override
    public CatalogColumn getColumn( PolyXid xid, long tableId, String columnName ) throws GenericCatalogException, UnknownColumnException {
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
     * @param xid The transaction identifier
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @param tableName The name of the table
     * @param columnName The name of the column
     * @return A CatalogColumn
     * @throws UnknownColumnException If there is no column with this name in the specified table of the database and schema.
     */
    @Override
    public CatalogColumn getColumn( PolyXid xid, String databaseName, String schemaName, String tableName, String columnName ) throws GenericCatalogException, UnknownColumnException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getColumn( transactionHandler, databaseName, schemaName, tableName, columnName );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | UnknownTypeException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public long addColumn( PolyXid xid, String name, long tableId, int position, PolySqlType type, Integer length, Integer precision, boolean nullable, Encoding encoding, Collation collation, boolean forceDefault ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogTable table = Statements.getTable( transactionHandler, tableId );
            return Statements.addColumn( transactionHandler, name, table.id, position, type, length, precision, nullable, encoding, collation, forceDefault );
        } catch ( CatalogConnectionException | CatalogTransactionException | UnknownEncodingException | UnknownCollationException | GenericCatalogException | UnknownTableTypeException | UnknownTableException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the user with the specified name.
     *
     * @param username The username
     * @return The user
     * @throws UnknownUserException If there is no user with this name.
     */
    @Override
    public CatalogUser getUser( String username ) throws UnknownUserException {
        return new CatalogUser( 0, "pa", "" );
    }


    /**
     * Get list of all stores
     *
     * @return List of stores
     */
    public List<CatalogStore> getStores( PolyXid xid ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return Statements.getStores( transactionHandler );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public CatalogCombinedDatabase getCombinedDatabase( PolyXid xid, long databaseId ) throws GenericCatalogException, UnknownSchemaException, UnknownTableException {
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
    public CatalogCombinedSchema getCombinedSchema( PolyXid xid, long schemaId ) throws GenericCatalogException, UnknownSchemaException, UnknownTableException {
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
    public CatalogCombinedTable getCombinedTable( PolyXid xid, long tableId ) throws GenericCatalogException, UnknownTableException {
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
            return new CatalogCombinedTable( table, columns, schema, database, owner, placements );
        } catch ( UnknownEncodingException | UnknownCollationException | UnknownTypeException | GenericCatalogException | UnknownTableTypeException | UnknownSchemaTypeException | UnknownSchemaException | UnknownDatabaseException | UnknownUserException e ) {
            throw new GenericCatalogException( e );
        }
    }


    @Override
    public boolean prepare( PolyXid xid ) throws CatalogTransactionException {
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
    public void commit( PolyXid xid ) throws CatalogTransactionException {
        val transactionHandler = XATransactionHandler.getTransactionHandler( xid );
        if ( XATransactionHandler.hasTransactionHandler( xid ) ) {
            transactionHandler.commit();
        } else {
            // e.g. SELECT 1; commit;
            LOG.debug( "Unknown transaction handler. This is not necessarily a problem as long as the query has not initiated any catalog lookups." );
        }
    }


    @Override
    public void rollback( PolyXid xid ) throws CatalogTransactionException {
        val transactionHandler = XATransactionHandler.getTransactionHandler( xid );
        if ( XATransactionHandler.hasTransactionHandler( xid ) ) {
            transactionHandler.rollback();
        } else {
            // e.g. SELECT 1; commit;
            LOG.debug( "Unknown transaction handler. This is not necessarily a problem as long as the query has not initiated any catalog lookups." );
        }
    }

}
