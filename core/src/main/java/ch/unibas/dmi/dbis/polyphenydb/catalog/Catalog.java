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
import java.util.List;


/**
 *
 */
public interface Catalog {

    // internalName = null means generate one
    CatalogDatabase addDatabase( PolyXid xid, InternalName internalName, String databaseName, CatalogUser ownerName, Encoding encoding, Collation collation, int connectionLimit ) throws CatalogConnectionException, CatalogTransactionException, UnknownUserException, DatabaseAlreadyExistsException, GenericCatalogException, ExceedsMaximumNumberOfDatabasesException;

    CatalogSchema addSchema( PolyXid xid, InternalName internalName, CatalogDatabase database, String schemaName, CatalogUser ownerName, Encoding encoding, Collation collation ) throws ExceedsMaximumNumberOfSchemasException, CatalogConnectionException, CatalogTransactionException, UnknownUserException, SchemaAlreadyExistsException, GenericCatalogException;

    CatalogTable addTable( PolyXid xid, InternalName internalName, CatalogDatabase database, CatalogSchema schema, String tableName, CatalogUser ownerName, Encoding encoding, Collation collation, TableType tableType, String tableDefinition ) throws CatalogConnectionException, CatalogTransactionException, TableAlreadyExistsException, UnknownUserException, ExceedsMaximumNumberOfTablesException, GenericCatalogException;

    CatalogColumn addColumn( PolyXid xid, InternalName internalName, CatalogDatabase database, CatalogSchema schema, CatalogTable table, String columnName, PolySqlType type, int position, boolean nullable, java.io.Serializable defaultValue, boolean forceDefault, Long autoincrementStartValue, Encoding encoding, Collation collation, Integer length, Integer precision ) throws CatalogConnectionException, CatalogTransactionException, GenericCatalogException, ColumnAlreadyExistsException, ExceedsMaximumNumberOfColumnsException;

    boolean deleteTable( PolyXid xid, InternalName internalName ) throws CatalogConnectionException, CatalogTransactionException, GenericCatalogException;

    void setChunkColumn( PolyXid xid, InternalName internalTableName, InternalName chunkColumn ) throws CatalogConnectionException, CatalogTransactionException, GenericCatalogException, UnknownTableException;

    void setChunkSize( PolyXid xid, InternalName internalTableName, Integer chunkSize ) throws CatalogConnectionException, CatalogTransactionException, GenericCatalogException, UnknownTableException;

    CatalogColumn getColumn( PolyXid xid, InternalName internalName ) throws GenericCatalogException, UnknownColumnException;

    CatalogTable getTable( PolyXid xid, InternalName internalName ) throws GenericCatalogException, UnknownTableException;

    CatalogSchema getSchema( PolyXid xid, InternalName internalName ) throws GenericCatalogException, UnknownSchemaException;

    CatalogDatabase getDatabase( PolyXid xid, InternalName internalName ) throws GenericCatalogException, UnknownDatabaseException;

    CatalogTable getTableFromName( PolyXid xid, final CatalogSchema schema, final String tableName ) throws UnknownTableException, GenericCatalogException;

    List<CatalogColumn> getAllColumnsForTable( PolyXid xid, CatalogTable table ) throws UnknownTableException, GenericCatalogException;

    List<CatalogTable> getAllTables( PolyXid xid ) throws GenericCatalogException;

    CatalogColumn getColumnFromName( PolyXid xid, CatalogTable table, String columnName ) throws UnknownColumnException, GenericCatalogException;

    CatalogUser loginUser( String username, String password ) throws UnknownUserException;

    CatalogUser getUserFromName( PolyXid xid, String userName ) throws UnknownUserException;

    CatalogDatabase getDatabaseFromName( String databaseName ) throws UnknownDatabaseException;

    CatalogSchema getSchemaFromName( CatalogDatabase database, String schemaName ) throws UnknownSchemaException;

    /*
     *
     */

    boolean prepare( PolyXid xid ) throws CatalogTransactionException;

    void commit( PolyXid xid ) throws CatalogTransactionException;

    void rollback( PolyXid xid ) throws CatalogTransactionException;


    /*
     *
     */


    enum TableType {
        TABLE( 1 );
        // VIEW, STREAM, ...

        private final int id;


        TableType( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static TableType getById( int id ) throws UnknownTableTypeException {
            for ( TableType t : values() ) {
                if ( t.id == id ) {
                    return t;
                }
            }
            throw new UnknownTableTypeException( id );
        }
    }


    enum Collation {
        CASE_SENSITIVE( 1 ),
        CASE_INSENSITIVE( 2 );

        private final int id;


        Collation( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static Collation getById( int id ) throws UnknownTableTypeException, UnknownEncodingException, UnknownCollationException {
            for ( Collation c : values() ) {
                if ( c.id == id ) {
                    return c;
                }
            }
            throw new UnknownCollationException( id );
        }
    }


    enum Encoding {
        UTF8( 1 );

        private final int id;


        Encoding( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static Encoding getById( int id ) throws UnknownTableTypeException, UnknownEncodingException {
            for ( Encoding e : values() ) {
                if ( e.id == id ) {
                    return e;
                }
            }
            throw new UnknownEncodingException( id );
        }
    }
}
