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
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogTransactionException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownColumnException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
import java.util.ArrayList;
import java.util.List;
import lombok.val;
import org.apache.calcite.avatica.Meta.Pat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class CatalogManagerImpl extends CatalogManager implements Catalog {

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


    @Override
    public Catalog getCatalog() {
        return this;
    }


    /**
     * Get all databases
     *
     * @param xid The transaction identifier
     * @return List of databases
     */
    @Override
    public List<CatalogDatabase> getDatabases( PolyXid xid ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );

            final List<CatalogDatabase> databases = new ArrayList<>();
            databases.add( new CatalogDatabase( "APP", "marco", Encoding.UTF8, Collation.CASE_INSENSITIVE, 0 ) );
            databases.add( new CatalogDatabase( "HR", "heiko", Encoding.UTF8, Collation.CASE_SENSITIVE, 10 ) );
            databases.add( new CatalogDatabase( "WEBSITE", "marco", Encoding.UTF8, Collation.CASE_INSENSITIVE, 0 ) );
            return databases;
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
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
            return new CatalogDatabase( "APP", "marco", Encoding.UTF8, Collation.CASE_INSENSITIVE, 0 );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all schemas of the specified database which fit to the specified filter pattern.
     * <code>getSchemas(xid, databaseName, null)</code> returns all schemas of the database.
     *
     * @param xid The transaction identifier
     * @param databaseName The name of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogSchema> getSchemas( PolyXid xid, String databaseName, Pat schemaNamePattern ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );

            final List<CatalogSchema> schemas = new ArrayList<>();
            schemas.add( new CatalogSchema( "public", "APP", "marco", Encoding.UTF8, Collation.CASE_INSENSITIVE, SchemaType.RELATIONAL ) );
            schemas.add( new CatalogSchema( "dev", "APP", "marco", Encoding.UTF8, Collation.CASE_INSENSITIVE, SchemaType.RELATIONAL ) );
            schemas.add( new CatalogSchema( "bar", "APP", "marco", Encoding.UTF8, Collation.CASE_SENSITIVE, SchemaType.RELATIONAL ) );
            return schemas;
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
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

            return new CatalogSchema( "public", "APP", "marco", Encoding.UTF8, Collation.CASE_INSENSITIVE, SchemaType.RELATIONAL );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all tables of the specified database which fit to the specified filters.
     * <code>getTables(xid, databaseName, null, null, null)</code> returns all tables of the database.
     *
     * @param xid The transaction identifier
     * @param databaseName The name of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all
     * @param tableNamePattern Pattern for the table name. null returns all
     * @param typeList List of table types to consider. null returns all
     * @return List of tables which fit to the specified filters. If there is no table which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogTable> getTables( PolyXid xid, String databaseName, Pat schemaNamePattern, Pat tableNamePattern, List<TableType> typeList ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );

            final List<CatalogTable> tables = new ArrayList<>();
            tables.add( new CatalogTable( "user", "public", "foo", "marco", Encoding.UTF8, Collation.CASE_INSENSITIVE, TableType.TABLE, "" ) );
            tables.add( new CatalogTable( "bid", "public", "foo", "alex", Encoding.UTF8, Collation.CASE_INSENSITIVE, TableType.TABLE, "" ) );
            tables.add( new CatalogTable( "category", "public", "foo", "alex", Encoding.UTF8, Collation.CASE_SENSITIVE, TableType.TABLE, null ) );
            tables.add( new CatalogTable( "auction", "public", "foo", "marco", Encoding.UTF8, Collation.CASE_INSENSITIVE, TableType.TABLE, "" ) );
            tables.add( new CatalogTable( "picture", "public", "foo", "marco", Encoding.UTF8, Collation.CASE_INSENSITIVE, TableType.TABLE, null ) );

            return tables;
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
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
            return new CatalogTable( "picture", "public", "foo", "marco", Encoding.UTF8, Collation.CASE_INSENSITIVE, TableType.TABLE, null );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Get all columns of the specified database which fit to the specified filter patterns.
     * <code>getColumns(xid, databaseName, null, null, null)</code> returns all columns of the database.
     *
     * @param xid The transaction identifier
     * @param databaseName The name of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all
     * @param tableNamePattern Pattern for the table name. null returns all
     * @param columnNamePattern Pattern for the column name. null returns all
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    @Override
    public List<CatalogColumn> getColumns( PolyXid xid, String databaseName, Pat schemaNamePattern, Pat tableNamePattern, Pat columnNamePattern ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            final List<CatalogColumn> columns = new ArrayList<>();
            columns.add( new CatalogColumn( "id", "user", "public", "foo", 0, PolySqlType.BIGINT, null, null, false, null, null, 0L, 1L, null, false ) );
            columns.add( new CatalogColumn( "email", "user", "public", "foo", 1, PolySqlType.VARCHAR, null, null, false, Encoding.UTF8, Collation.CASE_INSENSITIVE, null, null, null, false ) );
            columns.add( new CatalogColumn( "password", "user", "public", "foo", 2, PolySqlType.VARCHAR, null, null, false, Encoding.UTF8, Collation.CASE_SENSITIVE, null, null, null, false ) );
            columns.add( new CatalogColumn( "birthday", "user", "public", "foo", 3, PolySqlType.DATE, null, null, false, null, null, null, null, null, false ) );
            return columns;
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the column with the specified name in the specified table of the specified database and schema.
     *
     * @param xid The transaction identifier
     * @param databaseName The name of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all
     * @param tableNamePattern Pattern for the table name. null returns all
     * @param columnName The name of the column
     * @return The column
     * @throws UnknownColumnException If there is no column with this name in the specified database and schema.
     */
    @Override
    public CatalogColumn getColumn( PolyXid xid, String databaseName, Pat schemaNamePattern, Pat tableNamePattern, String columnName ) throws GenericCatalogException, UnknownColumnException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return new CatalogColumn( "id", "user", "public", "foo", 0, PolySqlType.BIGINT, null, null, false, null, null, 0L, 1L, null, false );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( e );
        }
    }


    /**
     * Returns the user with the specified name.
     *
     * @param userName The name of the database
     * @return The user
     * @throws UnknownUserException If there is no user with this name.
     */
    @Override
    public CatalogUser getUser( String userName ) throws UnknownUserException {
        return new CatalogUser( "pa", "" );
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
