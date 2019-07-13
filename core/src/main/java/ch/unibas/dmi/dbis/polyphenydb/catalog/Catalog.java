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


import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.CatalogTransactionException;
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
import java.util.ArrayList;
import java.util.List;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.avatica.Meta.Pat;


/**
 *
 */
public interface Catalog {

    /**
     * Get all databases
     *
     * @param xid The transaction identifier
     * @return List of databases
     */
    List<CatalogDatabase> getDatabases( PolyXid xid ) throws GenericCatalogException;


    /**
     * Returns the database with the given name.
     *
     * @param xid The transaction identifier
     * @param databaseName The name of the database
     * @return The database
     * @throws UnknownDatabaseException If there is no database with this name.
     */
    CatalogDatabase getDatabase( PolyXid xid, String databaseName ) throws GenericCatalogException, UnknownDatabaseException;


    /**
     * Get all schemas of the specified database which fit to the specified filter pattern.
     * <code>getSchemas(xid, databaseName, null)</code> returns all schemas of the database.
     *
     * @param xid The transaction identifier
     * @param databaseName The name of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all
     * @return List of schemas which fit to the specified filter. If there is no schema which meets the criteria, an empty list is returned.
     */
    List<CatalogSchema> getSchemas( PolyXid xid, String databaseName, Pat schemaNamePattern ) throws GenericCatalogException;


    /**
     * Returns the schema with the given name in the specified database.
     *
     * @param xid The transaction identifier
     * @param databaseName The name of the database
     * @param schemaName The name of the schema
     * @return The schema
     * @throws UnknownSchemaException If there is no schema with this name in the specified database.
     */
    CatalogSchema getSchema( PolyXid xid, String databaseName, String schemaName ) throws GenericCatalogException, UnknownSchemaException;


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
    List<CatalogTable> getTables( PolyXid xid, String databaseName, Pat schemaNamePattern, Pat tableNamePattern, List<TableType> typeList ) throws GenericCatalogException;


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
    CatalogTable getTable( PolyXid xid, String databaseName, String schemaName, String tableName ) throws UnknownTableException, GenericCatalogException;


    /**
     * Get all columns of the specified database which fit to the specified filter patterns.
     * <code>getColumns(xid, databaseName, null, null, null</code> returns all columns of the database.
     *
     * @param xid The transaction identifier
     * @param databaseName The name of the database
     * @param schemaNamePattern Pattern for the schema name. null returns all
     * @param tableNamePattern Pattern for the table name. null returns all
     * @param columnNamePattern Pattern for the column name. null returns all
     * @return List of columns which fit to the specified filters. If there is no column which meets the criteria, an empty list is returned.
     */
    List<CatalogColumn> getColumns( PolyXid xid, String databaseName, Pat schemaNamePattern, Pat tableNamePattern, Pat columnNamePattern ) throws GenericCatalogException;


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
    CatalogColumn getColumn( PolyXid xid, String databaseName, Pat schemaNamePattern, Pat tableNamePattern, String columnName ) throws GenericCatalogException, UnknownColumnException;


    /**
     * Returns the user with the specified name.
     *
     * @param userName The name of the database
     * @return The user
     * @throws UnknownUserException If there is no user with this name.
     */
    CatalogUser getUser( String userName ) throws UnknownUserException;


    /*
     *
     */

    CatalogCombinedSchema getCombinedSchema( String hsqldb );



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


        public static TableType getById( final int id ) throws UnknownTableTypeException {
            for ( TableType t : values() ) {
                if ( t.id == id ) {
                    return t;
                }
            }
            throw new UnknownTableTypeException( id );
        }


        public static TableType getByName( final String name ) throws UnknownTableTypeException {
            for ( TableType t : values() ) {
                if ( t.name().equalsIgnoreCase( name ) ) {
                    return t;
                }
            }
            throw new UnknownTableTypeException( name );
        }


        // Used for creating ResultSets
        public Object[] getParameterArray() {
            return new Object[]{ name() };
        }


        // Required for building JDBC result set
        @RequiredArgsConstructor
        public class PrimitiveTableType {

            public final String tableType;
        }
    }


    enum SchemaType {
        RELATIONAL( 1 );
        // GRAPH, DOCUMENT, ...

        private final int id;


        SchemaType( int id ) {
            this.id = id;
        }


        public int getId() {
            return id;
        }


        public static SchemaType getById( final int id ) throws UnknownSchemaTypeException {
            for ( SchemaType t : values() ) {
                if ( t.id == id ) {
                    return t;
                }
            }
            throw new UnknownSchemaTypeException( id );
        }


        public static SchemaType getByName( final String name ) throws UnknownSchemaTypeException {
            for ( SchemaType t : values() ) {
                if ( t.name().equalsIgnoreCase( name ) ) {
                    return t;
                }
            }
            throw new UnknownSchemaTypeException( name );
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


        public static Collation getById( int id ) throws UnknownCollationException {
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


        public static Encoding getById( int id ) throws UnknownEncodingException {
            for ( Encoding e : values() ) {
                if ( e.id == id ) {
                    return e;
                }
            }
            throw new UnknownEncodingException( id );
        }
    }


    /*
     * Helpers
     */

    static List<TableType> convertTableTypeList( @NonNull final List<String> stringTypeList ) throws UnknownTableTypeException {
        final List<TableType> typeList = new ArrayList<>( stringTypeList.size() );
        for ( String s : stringTypeList ) {
            typeList.add( TableType.getByName( s ) );
        }
        return typeList;
    }



}
