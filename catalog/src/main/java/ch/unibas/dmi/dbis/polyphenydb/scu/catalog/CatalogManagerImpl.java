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
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManager;
import ch.unibas.dmi.dbis.polyphenydb.catalog.InternalName;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogConnectionException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogTransactionException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.ColumnAlreadyExistsException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.DatabaseAlreadyExistsException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.ExceedsMaximumNumberOfColumnsException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.ExceedsMaximumNumberOfDatabasesException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.ExceedsMaximumNumberOfSchemasException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.ExceedsMaximumNumberOfTablesException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.SchemaAlreadyExistsException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.TableAlreadyExistsException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownColumnException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownEncodingException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
import java.sql.SQLException;
import java.util.List;
import lombok.NonNull;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class CatalogManagerImpl extends CatalogManager implements Catalog {

    private static final Logger logger = LoggerFactory.getLogger( CatalogManagerImpl.class );

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
                logger.error( "Exception while creating catalog schema", e );
                try {
                    if ( transactionHandler != null ) {
                        transactionHandler.rollback();
                    }
                } catch ( CatalogTransactionException e1 ) {
                    logger.error( "Exception while rollback", e );
                }
            }
        }
    }


    @Override
    public Catalog getCatalog() {
        return this;
    }


    @Override
    public CatalogDatabase addDatabase( @NonNull PolyXid xid, InternalName internalName, @NonNull String databaseName, @NonNull CatalogUser user, Encoding encoding, Collation collation, int connectionLimit ) throws CatalogConnectionException, CatalogTransactionException, UnknownUserException, DatabaseAlreadyExistsException, GenericCatalogException, ExceedsMaximumNumberOfDatabasesException {

        val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );

        databaseName = databaseName.trim().toLowerCase();
        val ownerName = user.getName().trim().toLowerCase();

        // Checks
        if ( !isValidIdentifier( databaseName ) ) {
            throw new GenericCatalogException( "The specified database name is not valid: " + databaseName );
        }
        if ( !isValidIdentifier( ownerName ) ) {
            throw new GenericCatalogException( "The specified owner name is not valid: " + ownerName );
        }

        try {
            // get owner id --- TODO: redo w/ the usage of the UserPrincipal class!
            int ownerId = Statements.getUserId( transactionHandler, ownerName );

            // check that there is not already a database with that name
            if ( Statements.checkIfDatabaseExists( transactionHandler, databaseName ) ) {
                throw new DatabaseAlreadyExistsException();
            }

            // Generate internal name if no internal name has been provided
            if ( internalName == null ) {
                internalName = Statements.getFreeDatabaseInternalName( transactionHandler );
            }

            // add database record to catalog
            if ( !Statements.addDatabase( transactionHandler, internalName, databaseName, ownerId, encoding, collation, connectionLimit ) ) {
                throw new GenericCatalogException( "Something went wrong... It was not possible to create record for the database " + databaseName );
            }

            return new CatalogDatabase( internalName, databaseName, ownerId, encoding, collation, connectionLimit );
        } catch ( SQLException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public CatalogSchema addSchema( @NonNull final PolyXid xid, InternalName internalName, @NonNull CatalogDatabase database, @NonNull String schemaName, @NonNull CatalogUser user, final Encoding encoding, final Collation collation ) throws ExceedsMaximumNumberOfSchemasException, CatalogConnectionException, CatalogTransactionException, UnknownUserException, SchemaAlreadyExistsException, GenericCatalogException {

        val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );

        schemaName = schemaName.trim().toLowerCase();
        val ownerName = user.getName().trim().toLowerCase();

        // Checks
        if ( !isValidIdentifier( schemaName ) ) {
            throw new GenericCatalogException( "The specified schema name is not valid: " + schemaName );
        }
        if ( !isValidIdentifier( ownerName ) ) {
            throw new GenericCatalogException( "The specified owner name is not valid: " + ownerName );
        }

        try {
            // get owner id --- TODO: see above
            int ownerId = Statements.getUserId( transactionHandler, ownerName );

            // check that there is not already a schema with that name
            if ( Statements.checkIfSchemaExists( transactionHandler, schemaName, database.getInternalName() ) ) {
                throw new SchemaAlreadyExistsException();
            }

            // Generate internal name if no internal name has been provided
            if ( internalName == null ) {
                internalName = Statements.getFreeSchemaInternalName( transactionHandler, database.getInternalName() );
            }

            // add schema record to catalog
            if ( !Statements.addSchema( transactionHandler, internalName, schemaName, ownerId, encoding, collation ) ) {
                throw new GenericCatalogException( "Something went wrong... It was not possible to create record for the schema " + schemaName );
            }

            return new CatalogSchema( internalName, schemaName, ownerId, encoding, collation );
        } catch ( SQLException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public CatalogTable addTable( @NonNull final PolyXid xid, InternalName internalName, @NonNull CatalogDatabase database, @NonNull CatalogSchema schema, @NonNull String tableName, @NonNull CatalogUser user, final Encoding encoding, final Collation collation, @NonNull final TableType tableType, String tableDefinition )
            throws CatalogConnectionException, CatalogTransactionException, TableAlreadyExistsException, UnknownUserException, ExceedsMaximumNumberOfTablesException, GenericCatalogException {

        val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );

        tableName = tableName.trim().toLowerCase();
        val ownerName = user.getName().trim().toLowerCase();

        // Checks
        if ( !isValidIdentifier( tableName ) ) {
            throw new GenericCatalogException( "The specified table name is not valid: " + tableName );
        }
        if ( !isValidIdentifier( ownerName ) ) {
            throw new GenericCatalogException( "The specified owner name is not valid: " + ownerName );
        }

        try {
            // get user id --- TODO: same ...
            int ownerId = Statements.getUserId( transactionHandler, ownerName );

            // check that there is not already a table with that name
            if ( Statements.checkIfTableExists( transactionHandler, tableName, schema.getInternalName() ) ) {
                throw new TableAlreadyExistsException();
            }

            // Generate internal name if no internal name has been provided
            if ( internalName == null ) {
                internalName = Statements.getFreeTableInternalName( transactionHandler, schema.getInternalName() );
            }

            // add table record to catalog
            if ( !Statements.addTable( transactionHandler, internalName, tableName, ownerId, encoding, collation, tableType, tableDefinition ) ) {
                throw new GenericCatalogException( "Something went wrong... It was not possible to create record for the table " + tableName );
            }

            return new CatalogTable( internalName, tableName, ownerId, encoding, collation, tableType, tableDefinition, null, null );
        } catch ( SQLException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public CatalogColumn addColumn( @NonNull final PolyXid xid, InternalName internalName, @NonNull CatalogDatabase database, @NonNull CatalogSchema schema, @NonNull CatalogTable table, @NonNull String columnName, @NonNull final PolySqlType type, int position, boolean nullable, java.io.Serializable defaultValue, boolean forceDefault, Long autoincrementStartValue, final Encoding encoding, final Collation collation, Integer length, Integer precision ) throws CatalogConnectionException, CatalogTransactionException, GenericCatalogException, ColumnAlreadyExistsException, ExceedsMaximumNumberOfColumnsException {

        val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );

        columnName = columnName.trim().toLowerCase();

        // checks
        if ( !isValidIdentifier( columnName ) ) {
            throw new GenericCatalogException( "The specified column name name is not valid: " + columnName );
        }

        try {
            // check that there is not already a column with this name in that table
            if ( Statements.checkIfColumnExists( transactionHandler, columnName, table.getInternalName() ) ) {
                throw new ColumnAlreadyExistsException( columnName, table.getName() );
            }

            // Generate internal name if no internal name has been provided
            if ( internalName == null ) {
                internalName = Statements.getFreeColumnInternalName( transactionHandler, table.getInternalName() );
            }

            @SuppressWarnings("UnnecessaryLocalVariable")
            Long autoincrementNextValue = autoincrementStartValue;

            if ( !Statements.addColumn( transactionHandler, internalName, columnName, type, position, nullable, defaultValue, forceDefault, autoincrementStartValue, autoincrementNextValue, encoding, collation, length, precision ) ) {
                throw new GenericCatalogException( "Something went wrong... It was not possible to create record for the column " + columnName );
            }

            return new CatalogColumn( internalName, columnName, position, type, length, precision, nullable, encoding, collation, autoincrementStartValue, autoincrementNextValue, defaultValue, forceDefault );
        } catch ( SQLException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public boolean deleteTable( @NonNull final PolyXid xid, @NonNull InternalName table ) throws CatalogConnectionException, CatalogTransactionException, GenericCatalogException {
        val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
        try {
            if ( Statements.deleteColumnsFromTable( transactionHandler, table ) ) {
                return Statements.deleteTable( transactionHandler, table );
            } else {
                throw new GenericCatalogException( "Unable to delete column records from catalog." );
            }
        } catch ( SQLException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public void setChunkColumn( @NonNull final PolyXid xid, @NonNull InternalName table, InternalName column ) throws CatalogConnectionException, CatalogTransactionException, GenericCatalogException, UnknownTableException {

        val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );

        try {
            // Check if table exists
            getTable( transactionHandler, table );

            // add schema record to catalog
            if ( !Statements.setChunkColumn( transactionHandler, table, column ) ) {
                throw new GenericCatalogException( "Something went wrong... It was not possible to set chunk column for table " + table );
            }

        } catch ( SQLException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public void setChunkSize( @NonNull final PolyXid xid, @NonNull InternalName table, Integer chunkSize ) throws CatalogConnectionException, CatalogTransactionException, GenericCatalogException, UnknownTableException {

        val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );

        try {
            // Check if table exists
            getTable( transactionHandler, table );

            // add schema record to catalog
            if ( !Statements.setChunkSize( transactionHandler, table, chunkSize ) ) {
                throw new GenericCatalogException( "Something went wrong... It was not possible to set chunk size for table " + table );
            }

        } catch ( SQLException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public CatalogColumn getColumn( @NonNull final PolyXid xid, @NonNull final InternalName internalName ) throws GenericCatalogException, UnknownColumnException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogColumn column = Statements.getColumn( transactionHandler, internalName );
            return column;
        } catch ( SQLException | CatalogConnectionException | UnknownEncodingException | UnknownCollationException | UnknownTypeException | UnknownTableTypeException | CatalogTransactionException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public CatalogTable getTable( @NonNull final PolyXid xid, @NonNull final InternalName internalName ) throws GenericCatalogException, UnknownTableException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            return getTable( transactionHandler, internalName );
        } catch ( CatalogConnectionException | CatalogTransactionException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    private CatalogTable getTable( @NonNull final TransactionHandler transactionHandler, @NonNull final InternalName internalName ) throws GenericCatalogException, UnknownTableException {
        try {
            return Statements.getTable( transactionHandler, internalName );
        } catch ( SQLException | UnknownEncodingException | UnknownCollationException | UnknownTableTypeException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public CatalogSchema getSchema( @NonNull final PolyXid xid, @NonNull final InternalName internalName ) throws GenericCatalogException, UnknownSchemaException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogSchema schema = Statements.getSchema( transactionHandler, internalName );
            return schema;
        } catch ( SQLException | CatalogConnectionException | UnknownEncodingException | UnknownCollationException | UnknownTableTypeException | CatalogTransactionException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public CatalogDatabase getDatabase( @NonNull final PolyXid xid, @NonNull final InternalName internalName ) throws GenericCatalogException, UnknownDatabaseException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            CatalogDatabase database = Statements.getDatabase( transactionHandler, internalName );
            return database;
        } catch ( SQLException | CatalogConnectionException | UnknownEncodingException | UnknownCollationException | UnknownTableTypeException | CatalogTransactionException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public CatalogTable getTableFromName( @NonNull final PolyXid xid, @NonNull final CatalogSchema schema, @NonNull final String tableName ) throws UnknownTableException, GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            val tableInternalName = Statements.getTableInternalName( transactionHandler, tableName, schema.getInternalName() );
            CatalogTable catalogTable = Statements.getTable( transactionHandler, tableInternalName );
            return catalogTable;
        } catch ( SQLException | CatalogConnectionException | UnknownEncodingException | UnknownCollationException | UnknownTableTypeException | CatalogTransactionException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public List<CatalogColumn> getAllColumnsForTable( @NonNull final PolyXid xid, @NonNull final CatalogTable table ) throws GenericCatalogException, UnknownTableException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            List<CatalogColumn> columns = Statements.getAllColumnsOfTable( transactionHandler, table.getInternalName() );
            return columns;
        } catch ( SQLException | CatalogConnectionException | UnknownEncodingException | UnknownCollationException | UnknownTypeException | UnknownTableTypeException | CatalogTransactionException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public List<CatalogTable> getAllTables( @NonNull final PolyXid xid ) throws GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            List<CatalogTable> columns = Statements.getAllTables( transactionHandler );
            return columns;
        } catch ( SQLException | CatalogConnectionException | UnknownEncodingException | UnknownCollationException | UnknownTableTypeException | CatalogTransactionException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public CatalogColumn getColumnFromName( @NonNull final PolyXid xid, @NonNull final CatalogTable table, @NonNull final String columnName ) throws UnknownColumnException, GenericCatalogException {
        try {
            val transactionHandler = XATransactionHandler.getOrCreateTransactionHandler( xid );
            val columnInternalName = Statements.getColumnInternalName( transactionHandler, columnName, table.getInternalName() );
            CatalogColumn catalogColumn = Statements.getColumn( transactionHandler, columnInternalName );
            return catalogColumn;
        } catch ( SQLException | CatalogConnectionException | UnknownEncodingException | UnknownCollationException | UnknownTypeException | UnknownTableTypeException | CatalogTransactionException e ) {
            throw new GenericCatalogException( "Something went wrong...", e );
        }
    }


    @Override
    public CatalogUser loginUser( String username, String password ) throws UnknownUserException {
        logger.warn( "Returning the user 'pa' since this function is not properly implemented yet." );
        return new CatalogUser( "pa", 1 ); // TODO
    }


    @Override
    public CatalogUser getUserFromName( @NonNull final PolyXid xid, String userName ) throws UnknownUserException {
        logger.warn( "Returning the user '{}' directly since this function is not properly implemented yet.", userName );
        return new CatalogUser( "pa", 1 ); // TODO
    }


    @Override
    public CatalogDatabase getDatabaseFromName( String dbName ) throws UnknownDatabaseException {
        logger.warn( "Mock implementation. Implement this function properly!" );
        return new CatalogDatabase( InternalName.fromString( "aa" ), dbName, 1, Encoding.UTF8, Collation.CASE_INSENSITIVE, 0 ); // TODO
    }


    @Override
    public CatalogSchema getSchemaFromName( CatalogDatabase database, String schemaName ) throws UnknownSchemaException {
        logger.warn( "Mock implementation. Implement this function properly!" );
        return new CatalogSchema( InternalName.fromString( "aaaa" ), schemaName, 1, Encoding.UTF8, Collation.CASE_INSENSITIVE ); // TODO
    }


    @Override
    public boolean prepare( PolyXid xid ) throws CatalogTransactionException {
        val transactionHandler = XATransactionHandler.getTransactionHandler( xid );
        if ( XATransactionHandler.hasTransactionHandler( xid ) ) {
            return transactionHandler.prepare();
        } else {
            // e.g. SELECT 1; commit;
            logger.debug( "Unknown transaction handler. This is not necessarily a problem as long as the query has not initiated any catalog lookups." );
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
            logger.debug( "Unknown transaction handler. This is not necessarily a problem as long as the query has not initiated any catalog lookups." );
        }
    }


    @Override
    public void rollback( PolyXid xid ) throws CatalogTransactionException {
        val transactionHandler = XATransactionHandler.getTransactionHandler( xid );
        if ( XATransactionHandler.hasTransactionHandler( xid ) ) {
            transactionHandler.rollback();
        } else {
            // e.g. SELECT 1; commit;
            logger.debug( "Unknown transaction handler. This is not necessarily a problem as long as the query has not initiated any catalog lookups." );
        }
    }

}
