/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.avatica;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MetaImpl.MetaTypeInfo;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.apache.calcite.avatica.remote.ProtobufMeta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.Unsafe;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.Catalog.EntityType.PrimitiveTableType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.Catalog.Pattern;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogColumn.PrimitiveCatalogColumn;
import org.polypheny.db.catalog.entity.CatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogDatabase.PrimitiveCatalogDatabase;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogForeignKey.CatalogForeignKeyColumn;
import org.polypheny.db.catalog.entity.CatalogForeignKey.CatalogForeignKeyColumn.PrimitiveCatalogForeignKeyColumn;
import org.polypheny.db.catalog.entity.CatalogIndex;
import org.polypheny.db.catalog.entity.CatalogIndex.CatalogIndexColumn;
import org.polypheny.db.catalog.entity.CatalogIndex.CatalogIndexColumn.PrimitiveCatalogIndexColumn;
import org.polypheny.db.catalog.entity.CatalogObject;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey.CatalogPrimaryKeyColumn;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey.CatalogPrimaryKeyColumn.PrimitiveCatalogPrimaryKeyColumn;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogSchema.PrimitiveCatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogTable.PrimitiveCatalogTable;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.LimitIterator;
import org.polypheny.db.util.Pair;


@Slf4j
public class DbmsMeta implements ProtobufMeta {

    /**
     * Special value for {@code Statement#getLargeMaxRows()} that means fetch an unlimited number of rows in a single batch.
     *
     * Any other negative value will return an unlimited number of rows but will do it in the default batch size, namely 100.
     */
    public static final int UNLIMITED_COUNT = -2;

    public static final boolean SEND_FIRST_FRAME_WITH_RESPONSE = false;

    private final ConcurrentMap<String, PolyphenyDbConnectionHandle> openConnections = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, PolyphenyDbStatementHandle> openStatements = new ConcurrentHashMap<>();

    final Calendar calendar = Unsafe.localCalendar();
    private final Catalog catalog = Catalog.getInstance();

    private final TransactionManager transactionManager;
    private final Authenticator authenticator;

    private final MonitoringPage monitoringPage;

    /**
     * Generates ids for statements. The ids are unique across all connections created by this DbmsMeta.
     */
    private final AtomicInteger statementIdGenerator = new AtomicInteger();


    /**
     * Creates a DbmsMeta
     */
    DbmsMeta( TransactionManager transactionManager, Authenticator authenticator, String uniqueName ) {
        this.transactionManager = transactionManager;
        this.authenticator = authenticator;

        // Add information page
        monitoringPage = new MonitoringPage( uniqueName );
    }


    public void shutdown() {
        monitoringPage.remove();
    }


    private static Object addProperty( final Map<DatabaseProperty, Object> map, final DatabaseMetaData metaData, final DatabaseProperty p ) throws SQLException {
        Object propertyValue;
        if ( p.isJdbc ) {
            try {
                propertyValue = p.method.invoke( metaData );
            } catch ( IllegalAccessException | InvocationTargetException e ) {
                throw new RuntimeException( e );
            }
        } else {
            propertyValue = p.defaultValue;
        }

        return map.put( p, propertyValue );
    }


    private MetaResultSet createMetaResultSet(
            final ConnectionHandle ch,
            final StatementHandle statementHandle,
            Map<String, Object> internalParameters,
            List<ColumnMetaData> columns,
            CursorFactory cursorFactory,
            final Frame firstFrame ) {
        final PolyphenyDbSignature<Object> signature =
                new PolyphenyDbSignature<Object>(
                        "",
                        ImmutableList.of(),
                        internalParameters,
                        null,
                        columns,
                        cursorFactory,
                        null,
                        ImmutableList.of(),
                        -1,
                        null,
                        StatementType.SELECT,
                        new ExecutionTimeMonitor(),
                        NamespaceType.RELATIONAL ) {
                    /*@Override
                    public Enumerable<Object> enumerable( DataContext dataContext ) {
                        return Linq4j.asEnumerable( firstFrame.rows );
                    }*/
                };
        return MetaResultSet.create( ch.id, statementHandle.id, true, signature, firstFrame );
    }


    private <E> MetaResultSet createMetaResultSet( final ConnectionHandle ch, final StatementHandle statementHandle, Enumerable<E> enumerable, Class clazz, String... names ) {
        final List<ColumnMetaData> columns = new ArrayList<>();
        final List<Field> fields = new ArrayList<>();
        //final List<String> fieldNames = new ArrayList<>();
        for ( String name : names ) {
            final int index = fields.size();
            final String fieldName = AvaticaUtils.toCamelCase( name );
            final Field field;
            try {
                field = clazz.getField( fieldName );
            } catch ( NoSuchFieldException e ) {
                throw new RuntimeException( e );
            }
            columns.add( MetaImpl.columnMetaData( name, index, field.getType(), false ) );
            fields.add( field );
            //fieldNames.add( fieldName );
        }
        //noinspection unchecked
        final Iterable<Object> iterable = (Iterable<Object>) enumerable;
        //return createMetaResultSet( ch, statementHandle, Collections.emptyMap(), columns, CursorFactory.record( clazz, fields, fieldNames ), new Frame( 0, true, iterable ) );
        return createMetaResultSet( ch, statementHandle, Collections.emptyMap(), columns, CursorFactory.LIST, new Frame( 0, true, iterable ) );
    }


    private Enumerable<Object> toEnumerable( final List<? extends CatalogObject> entities ) {
        final List<Object> objects = new LinkedList<>();
        for ( CatalogObject entity : entities ) {
            objects.add( entity.getParameterArray() );
        }
        return Linq4j.asEnumerable( objects );
    }


    /**
     * Returns a map of static database properties.
     *
     * The provider can omit properties whose value is the same as the default.
     */
    @Override
    public Map<DatabaseProperty, Object> getDatabaseProperties( ConnectionHandle ch ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getDatabaseProperties( ConnectionHandle {} )", ch );
            }

            final Map<DatabaseProperty, Object> map = new HashMap<>();
            // TODO

            log.error( "[NOT IMPLEMENTED YET] getDatabaseProperties( ConnectionHandle {} )", ch );
            return map;
        }
    }


    // TODO: typeList is ignored
    @Override
    public MetaResultSet getTables( final ConnectionHandle ch, final String database, final Pat schemaPattern, final Pat tablePattern, final List<String> typeList ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getTables( ConnectionHandle {}, String {}, Pat {}, Pat {}, List<String> {} )", ch, database, schemaPattern, tablePattern, typeList );
            }

            final List<CatalogTable> tables = catalog.getTables(
                    database == null ? null : new Pattern( database ),
                    (schemaPattern == null || schemaPattern.s == null) ? null : new Pattern( schemaPattern.s ),
                    (tablePattern == null || tablePattern.s == null) ? null : new Pattern( tablePattern.s )
            );
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( tables ),
                    PrimitiveCatalogTable.class,
                    // According to JDBC standard:
                    "TABLE_CAT",            // The name of the database in which the specified table resides.
                    "TABLE_SCHEM",                  // The name of the schema
                    "TABLE_NAME",                   // The name of the table
                    "TABLE_TYPE",                   // The type of the table (Table, View, ...)
                    "REMARKS",                      // The description of the table. --> Not used, always null
                    "TYPE_CAT",                     // Always null
                    "TYPE_SCHEM",                   // Always null
                    "TYPE_NAME",                    // Always null
                    "SELF_REFERENCING_COL_NAME",    // Name of the designated "identifier" column of a typed table  --> currently always null
                    "REF_GENERATION",               // How values in SELF_REFERENCING_COL_NAME are created. Values are "SYSTEM", "USER", "DERIVED".  --> currently always null
                    // Polypheny-DB specific extensions:
                    "OWNER"
            );
        }
    }


    @Override
    public MetaResultSet getColumns( final ConnectionHandle ch, final String database, final Pat schemaPattern, final Pat tablePattern, final Pat columnPattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, database, schemaPattern, tablePattern, columnPattern );
            }
            final List<CatalogColumn> columns = catalog.getColumns(
                    database == null ? null : new Pattern( database ),
                    (schemaPattern == null || schemaPattern.s == null) ? null : new Pattern( schemaPattern.s ),
                    (tablePattern == null || tablePattern.s == null) ? null : new Pattern( tablePattern.s ),
                    (columnPattern == null || columnPattern.s == null) ? null : new Pattern( columnPattern.s )
            );
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( columns ),
                    PrimitiveCatalogColumn.class,
                    // According to JDBC standard:
                    "TABLE_CAT",  // the database name
                    "TABLE_SCHEM",        // the schema name
                    "TABLE_NAME",         // the table name
                    "COLUMN_NAME",        // the column name
                    "DATA_TYPE",          // The SQL data type from java.sql.Types.
                    "TYPE_NAME",          // The name of the data type.
                    "COLUMN_SIZE",        // The length of the column (number of chars in a string or number of digits in a numerical data type).
                    "BUFFER_LENGTH",      // Transfer size of the data. --> not used, always null
                    "DECIMAL_DIGITS",     // The number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
                    "NUM_PREC_RADIX",     // The radix of the column. (typically either 10 or 2)
                    "NULLABLE",           // Indicates if the column is nullable. 1 means nullable
                    "REMARKS",            // The comments associated with the column. --> Polypheny-DB always returns null for this column
                    "COLUMN_DEF",         // The default value of the column.
                    "SQL_DATA_TYPE",      // This column is the same as the DATA_TYPE column, except for the datetime and SQL-92 interval data types. --> unused, always null
                    "SQL_DATETIME_SUB",   // Subtype code for datetime and SQL-92 interval data types. For other data types, this column returns NULL. --> unused, always null
                    "CHAR_OCTET_LENGTH",  // The maximum number of bytes in the column (only for char types) --> always null
                    "ORDINAL_POSITION",   // The index of the column within the table.
                    "IS_NULLABLE",        // Indicates if the column allows null values.
                    // Polypheny-DB specific extensions:
                    "COLLATION"
            );
        }
    }


    @Override
    public MetaResultSet getSchemas( final ConnectionHandle ch, final String database, final Pat schemaPattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getSchemas( ConnectionHandle {}, String {}, Pat {} )", ch, database, schemaPattern );
            }
            final List<CatalogSchema> schemas = catalog.getSchemas(
                    database == null ? null : new Pattern( database ),
                    (schemaPattern == null || schemaPattern.s == null) ? null : new Pattern( schemaPattern.s )
            );
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( schemas ),
                    PrimitiveCatalogSchema.class,
                    // According to JDBC standard:
                    "TABLE_SCHEM",
                    "TABLE_CATALOG",
                    // Polypheny-DB specific extensions:
                    "OWNER",
                    "SCHEMA_TYPE"
            );
        }
    }


    @Override
    public MetaResultSet getCatalogs( final ConnectionHandle ch ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getCatalogs( ConnectionHandle {} )", ch );
            }
            final List<CatalogDatabase> databases = catalog.getDatabases( null );
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( databases ),
                    PrimitiveCatalogDatabase.class,
                    // According to JDBC standard:
                    "TABLE_CAT",
                    // Polypheny-DB specific extensions:
                    "OWNER",
                    "DEFAULT_SCHEMA"
            );
        }
    }


    @Override
    public MetaResultSet getTableTypes( final ConnectionHandle ch ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getTableTypes( ConnectionHandle {} )", ch );
            }
            final List<Object> objects = new LinkedList<>();
            for ( EntityType tt : EntityType.values() ) {
                objects.add( tt.getParameterArray() );
            }
            Enumerable<Object> enumerable = Linq4j.asEnumerable( objects );
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    enumerable,
                    PrimitiveTableType.class,
                    "TABLE_TYPE"
            );
        }
    }


    @Override
    public MetaResultSet getProcedures( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat procedureNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getProcedures( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getProcedures( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern );
            return null;
        }
    }


    @Override
    public MetaResultSet getProcedureColumns( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat procedureNamePattern, final Pat columnNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getProcedureColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern, columnNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getProcedureColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern, columnNamePattern );
            return null;
        }
    }


    @Override
    public MetaResultSet getColumnPrivileges( final ConnectionHandle ch, final String catalog, final String schema, final String table, final Pat columnNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getColumnPrivileges( ConnectionHandle {}, String {}, String {}, String {}, Pat {} )", ch, catalog, schema, table, columnNamePattern );
            }

            // TODO

            log.error( "[NOT IMPLEMENTED YET] getColumnPrivileges( ConnectionHandle {}, String {}, String {}, String {}, Pat {} )", ch, catalog, schema, table, columnNamePattern );
            return null;
        }
    }


    @Override
    public MetaResultSet getTablePrivileges( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat tableNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getTablePrivileges( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
            }

            // TODO

            log.error( "[NOT IMPLEMENTED YET] getTablePrivileges( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
            return null;
        }
    }


    @Override
    public MetaResultSet getBestRowIdentifier( final ConnectionHandle ch, final String catalog, final String schema, final String table, final int scope, final boolean nullable ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getBestRowIdentifier( ConnectionHandle {}, String {}, String {}, String {}, int {}, boolean {} )", ch, catalog, schema, table, scope, nullable );
            }

            log.error( "[NOT IMPLEMENTED YET] getBestRowIdentifier( ConnectionHandle {}, String {}, String {}, String {}, int {}, boolean {} )", ch, catalog, schema, table, scope, nullable );
            return null;
        }
    }


    @Override
    public MetaResultSet getVersionColumns( final ConnectionHandle ch, final String catalog, final String schema, final String table ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getVersionColumns( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
            }

            log.error( "[NOT IMPLEMENTED YET] getVersionColumns( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
            return null;
        }
    }


    @Override
    public MetaResultSet getPrimaryKeys( final ConnectionHandle ch, final String database, final String schema, final String table ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getPrimaryKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, database, schema, table );
            }
            final Pattern tablePattern = table == null ? null : new Pattern( table );
            final Pattern schemaPattern = schema == null ? null : new Pattern( schema );
            final Pattern databasePattern = database == null ? null : new Pattern( database );
            final List<CatalogTable> catalogEntities = catalog.getTables( databasePattern, schemaPattern, tablePattern );
            List<CatalogPrimaryKeyColumn> primaryKeyColumns = new LinkedList<>();
            for ( CatalogTable catalogTable : catalogEntities ) {
                if ( catalogTable.primaryKey != null ) {
                    final CatalogPrimaryKey primaryKey = catalog.getPrimaryKey( catalogTable.primaryKey );
                    primaryKeyColumns.addAll( primaryKey.getCatalogPrimaryKeyColumns() );
                }
            }
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( primaryKeyColumns ),
                    PrimitiveCatalogPrimaryKeyColumn.class,
                    // According to JDBC standard:
                    "TABLE_CAT",  // database name
                    "TABLE_SCHEM",        // schema name
                    "TABLE_NAME",         // table name
                    "COLUMN_NAME",        // column name
                    "KEY_SEQ",            // Sequence number within primary key( a value of 1 represents the first column of the primary key, a value of 2 would represent the second column within the primary key).
                    "PK_NAME"             // the name of the primary key --> always null (primary keys have no name in Polypheny-DB)
            );
        }
    }


    @SuppressWarnings("Duplicates")
    @Override
    public MetaResultSet getImportedKeys( final ConnectionHandle ch, final String database, final String schema, final String table ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getImportedKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, database, schema, table );
            }
            final Pattern tablePattern = table == null ? null : new Pattern( table );
            final Pattern schemaPattern = schema == null ? null : new Pattern( schema );
            final Pattern databasePattern = database == null ? null : new Pattern( database );
            final List<CatalogTable> catalogEntities = catalog.getTables( databasePattern, schemaPattern, tablePattern );
            List<CatalogForeignKeyColumn> foreignKeyColumns = new LinkedList<>();
            for ( CatalogTable catalogTable : catalogEntities ) {
                List<CatalogForeignKey> importedKeys = catalog.getForeignKeys( catalogTable.id );
                importedKeys.forEach( catalogForeignKey -> foreignKeyColumns.addAll( catalogForeignKey.getCatalogForeignKeyColumns() ) );
            }
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( foreignKeyColumns ),
                    PrimitiveCatalogForeignKeyColumn.class,
                    // According to JDBC standard:
                    "PKTABLE_CAT",    // The name of the database that contains the table with the referenced primary key.
                    "PKTABLE_SCHEM",          // The name of the schema that contains the table with the referenced primary key.
                    "PKTABLE_NAME",           // The name of the table with the referenced primary key.
                    "PKCOLUMN_NAME",          // The column name of the primary key being imported.
                    "FKTABLE_CAT",            // The name of the database that contains the table with the foreign key.
                    "FKTABLE_SCHEM",          // The name of the schema that contains the table with the foreign key.
                    "FKTABLE_NAME",          // The name of the table containing the foreign key.
                    "FKCOLUMN_NAME",          // The column name of the foreign key.
                    "KEY_SEQ",                // The sequence number of the column in a multi-column primary key.
                    "UPDATE_RULE",            // What happens to a foreign key when the primary key is updated.
                    "DELETE_RULE",            // What happens to a foreign key when the primary key is deleted.
                    "FK_NAME",                // The name of the foreign key.
                    "PK_NAME",                // The name of the primary key.
                    "DEFERRABILITY"           // Indicates if the evaluation of the foreign key constraint can be deferred until a commit. --> always null
            );
        }
    }


    @SuppressWarnings("Duplicates")
    @Override
    public MetaResultSet getExportedKeys( final ConnectionHandle ch, final String database, final String schema, final String table ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getExportedKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, database, schema, table );
            }
            final Pattern tablePattern = table == null ? null : new Pattern( table );
            final Pattern schemaPattern = schema == null ? null : new Pattern( schema );
            final Pattern databasePattern = database == null ? null : new Pattern( database );
            final List<CatalogTable> catalogEntities = catalog.getTables( databasePattern, schemaPattern, tablePattern );
            List<CatalogForeignKeyColumn> foreignKeyColumns = new LinkedList<>();
            for ( CatalogTable catalogTable : catalogEntities ) {
                List<CatalogForeignKey> exportedKeys = catalog.getExportedKeys( catalogTable.id );
                exportedKeys.forEach( catalogForeignKey -> foreignKeyColumns.addAll( catalogForeignKey.getCatalogForeignKeyColumns() ) );
            }
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( foreignKeyColumns ),
                    PrimitiveCatalogForeignKeyColumn.class,
                    // According to JDBC standard:
                    "PKTABLE_CAT",            // The name of the database that contains the table with the referenced primary key.
                    "PKTABLE_SCHEM",          // The name of the schema that contains the table with the referenced primary key.
                    "PKTABLE_NAME",           // The name of the table with the referenced primary key.
                    "PKCOLUMN_NAME",          // The column name of the primary key being imported.
                    "FKTABLE_CAT",            // The name of the database that contains the table with the foreign key.
                    "FKTABLE_SCHEM",          // The name of the schema that contains the table with the foreign key.
                    "FKTABLE_NAME",           // The name of the table containing the foreign key.
                    "FKCOLUMN_NAME",          // The column name of the foreign key.
                    "KEY_SEQ",                // The sequence number of the column in a multi-column primary key.
                    "UPDATE_RULE",            // What happens to a foreign key when the primary key is updated.
                    "DELETE_RULE",            // What happens to a foreign key when the primary key is deleted.
                    "FK_NAME",                // The name of the foreign key.
                    "PK_NAME",                // The name of the primary key. --> always null
                    "DEFERRABILITY"           // Indicates if the evaluation of the foreign key constraint can be deferred until a commit.
            );
        }
    }


    @Override
    public MetaResultSet getCrossReference( final ConnectionHandle ch, final String parentCatalog, final String parentSchema, final String parentTable, final String foreignCatalog, final String foreignSchema, final String foreignTable ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getCrossReference( ConnectionHandle {}, String {}, String {}, String {}, String {}, String {}, String {} )", ch, parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable );
            }

            // TODO

            log.error( "[NOT IMPLEMENTED YET] getCrossReference( ConnectionHandle {}, String {}, String {}, String {}, String {}, String {}, String {} )", ch, parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable );
            return null;
        }
    }


    @Override
    public MetaResultSet getTypeInfo( final ConnectionHandle ch ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getTypeInfo( ConnectionHandle {} )", ch );
            }
            final StatementHandle statementHandle = createStatement( ch );
            final AlgDataTypeSystem typeSystem = AlgDataTypeSystem.DEFAULT;
            final List<Object> objects = new LinkedList<>();
            for ( PolyType polyType : PolyType.values() ) {
                objects.add(
                        new Serializable[]{
                                polyType.getName(),
                                polyType.getJdbcOrdinal(),
                                typeSystem.getMaxPrecision( polyType ),
                                typeSystem.getLiteral( polyType, true ),
                                typeSystem.getLiteral( polyType, false ),
                                null,
                                (short) DatabaseMetaData.typeNullable, // All types are nullable
                                typeSystem.isCaseSensitive( polyType ),
                                (short) DatabaseMetaData.typeSearchable, // Making all type searchable; we may want to be specific and declare under PolyType
                                false,
                                false,
                                typeSystem.isAutoincrement( polyType ),
                                polyType.getName(),
                                (short) polyType.getMinScale(),
                                (short) typeSystem.getMaxScale( polyType ),
                                null,
                                null,
                                typeSystem.getNumTypeRadix( polyType ) == 0 ? null : typeSystem.getNumTypeRadix( polyType ) } );
            }
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    Linq4j.asEnumerable( objects ),
                    MetaTypeInfo.class,
                    "TYPE_NAME",   // The name of the data type.
                    "DATA_TYPE",           // The SQL data type from java.sql.Types.
                    "PRECISION",           // The maximum number of significant digits.
                    "LITERAL_PREFIX",      // Prefix used to quote a literal
                    "LITERAL_SUFFIX",      // Suffix used to quote a literal
                    "CREATE_PARAMS",       // Parameters used in creating the type --> not used, always null
                    "NULLABLE",            // Indicates if the column can contain a null value (1: means type can contain null, 0 not). --> Currently 1 for all types
                    "CASE_SENSITIVE",      // Indicates if the data type is case sensitive. "true" if the type is case sensitive; otherwise, "false".
                    "SEARCHABLE",          // Indicates if (and how) the column can be used in a SQL WHERE clause. 0: none, 1: char, 2: basic, 3: searchable
                    "UNSIGNED_ATTRIBUTE",  // Indicates the sign of the data type. "true" if the type is unsigned; otherwise, "false". --> Currently false for all types
                    "FIXED_PREC_SCALE",    // Indicates that the data type can be a money value. "true" if the data type is money type; otherwise, "false".
                    "AUTO_INCREMENT",      // Indicates that the data type can be automatically incremented. "true" if the type can be auto incremented; otherwise, "false".
                    "LOCAL_TYPE_NAME",     // The localized name of the data type. --> Same as TYPE_NAME
                    "MINIMUM_SCALE",       // The maximum number of digits to the right of the decimal point.
                    "MAXIMUM_SCALE",       // The minimum number of digits to the right of the decimal point.
                    "SQL_DATA_TYPE",       // Not used, always null
                    "SQL_DATETIME_SUB",    // Not used, always null
                    "NUM_PREC_RADIX" );    // The radix
        }
    }


    @Override
    public MetaResultSet getIndexInfo( final ConnectionHandle ch, final String database, final String schema, final String table, final boolean unique, final boolean approximate ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getIndexInfo( ConnectionHandle {}, String {}, String {}, String {}, boolean {}, boolean {} )", ch, database, schema, table, unique, approximate );
            }
            final Pattern tablePattern = table == null ? null : new Pattern( table );
            final Pattern schemaPattern = schema == null ? null : new Pattern( schema );
            final Pattern databasePattern = database == null ? null : new Pattern( database );
            final List<CatalogTable> catalogEntities = catalog.getTables( databasePattern, schemaPattern, tablePattern );
            List<CatalogIndexColumn> catalogIndexColumns = new LinkedList<>();
            for ( CatalogTable catalogTable : catalogEntities ) {
                List<CatalogIndex> catalogIndexInfos = catalog.getIndexes( catalogTable.id, unique );
                catalogIndexInfos.forEach( info -> catalogIndexColumns.addAll( info.getCatalogIndexColumns() ) );
            }
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( catalogIndexColumns ),
                    PrimitiveCatalogIndexColumn.class,
                    // According to JDBC standard:
                    "TABLE_CAT",    // The name of the database in which the specified table resides.
                    "TABLE_SCHEM",          // The name of the schema in which the specified table resides.
                    "TABLE_NAME",           // The name of the table in which the index resides.
                    "NON_UNIQUE",           // Indicates whether the index values can be non-unique.
                    "INDEX_QUALIFIER",      // --> currently always returns null
                    "INDEX_NAME",           // The name of the index.
                    "TYPE",                 // The type of the index. (integer between 0 and 3) --> currently always returns 0
                    "ORDINAL_POSITION",     // The ordinal position of the column in the index. The first column in the index is 1.
                    "COLUMN_NAME",          // The name of the column.
                    "ASC_OR_DESC",          // The order used in the collation of the index.--> currently always returns null
                    "CARDINALITY",          // The number of rows in the table or unique values in the index. --> currently always returns -1
                    "PAGES",                // The number of pages used to store the index or table. --> currently always returns null
                    "FILTER_CONDITION",     // The filter condition. --> currently always returns null
                    // Polypheny-DB specific extensions
                    "LOCATION",             // On which store the index is located. NULL indicates a Polystore Index.
                    "INDEX_TYPE"            // Polypheny-DB specific index type
            );
        }
    }


    @Override
    public MetaResultSet getUDTs( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat typeNamePattern, final int[] types ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getUDTs( ConnectionHandle {}, String {}, Pat {}, Pat {}, int[] {} )", ch, catalog, schemaPattern, typeNamePattern, types );
            }

            log.error( "[NOT IMPLEMENTED YET] getUDTs( ConnectionHandle {}, String {}, Pat {}, Pat {}, int[] {} )", ch, catalog, schemaPattern, typeNamePattern, types );
            return null;
        }
    }


    @Override
    public MetaResultSet getSuperTypes( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat typeNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getSuperTypes( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getSuperTypes( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern );
            return null;
        }
    }


    @Override
    public MetaResultSet getSuperTables( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat tableNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getSuperTables( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getSuperTables( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
            return null;
        }
    }


    @Override
    public MetaResultSet getAttributes( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat typeNamePattern, final Pat attributeNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getAttributes( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern, attributeNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getAttributes( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern, attributeNamePattern );
            return null;
        }
    }


    @Override
    public MetaResultSet getClientInfoProperties( final ConnectionHandle ch ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getClientInfoProperties( ConnectionHandle {} )", ch );
            }

            log.error( "[NOT IMPLEMENTED YET] getClientInfoProperties( ConnectionHandle {} )", ch );
            return null;
        }
    }


    @Override
    public MetaResultSet getFunctions( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat functionNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getFunctions( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getFunctions( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern );
            return null;
        }
    }


    @Override
    public MetaResultSet getFunctionColumns( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat functionNamePattern, final Pat columnNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getFunctionColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern, columnNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getFunctionColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern, columnNamePattern );
            return null;
        }
    }


    @Override
    public MetaResultSet getPseudoColumns( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat tableNamePattern, final Pat columnNamePattern ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getPseudoColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern, columnNamePattern );
            }

            log.error( "[NOT IMPLEMENTED YET] getPseudoColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern, columnNamePattern );
            return null;
        }
    }


    /**
     * Executes a batch of commands on a prepared statement.
     *
     * @param h Statement handle
     * @param parameterValues A collection of list of typed values, one list per batch
     * @return An array of update counts containing one element for each command in the batch.
     */
    @Override
    public ExecuteBatchResult executeBatchProtobuf( final StatementHandle h, final List<UpdateBatch> parameterValues ) throws NoSuchStatementException {
        final PolyphenyDbConnectionHandle connection = openConnections.get( h.connectionId );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "executeBatchProtobuf( StatementHandle {}, List<UpdateBatch> {} )", h, parameterValues );
            }

            final PolyphenyDbStatementHandle statementHandle = getPolyphenyDbStatementHandle( h );

            long[] updateCounts = new long[parameterValues.size()];
            Map<Long, List<Object>> values = new HashMap<>();
            for ( UpdateBatch updateBatch : parameterValues ) {
                List<Common.TypedValue> list = updateBatch.getParameterValuesList();
                long index = 0;
                for ( Common.TypedValue v : list ) {
                    long i = index++;
                    if ( !values.containsKey( i ) ) {
                        values.put( i, new LinkedList<>() );
                    }
                    if ( "ARRAY".equals( v.getType().name() ) ) {
                        values.get( i ).add( convertList( (List<Object>) TypedValue.fromProto( v ).toLocal() ) );
                    } else {
                        values.get( i ).add( TypedValue.fromProto( v ).toJdbc( calendar ) );
                    }
                }
            }

            try {
                if ( values.size() == 0 ) {
                    // Nothing to execute
                    return new ExecuteBatchResult( new long[0] );
                }
                statementHandle.setStatement( connection.getCurrentOrCreateNewTransaction().createStatement() );
                for ( Entry<Long, List<Object>> valuesList : values.entrySet() ) {
                    statementHandle.getStatement().getDataContext().addParameterValues( valuesList.getKey(), null, valuesList.getValue() );
                }
                prepare( h, statementHandle.getPreparedQuery() );
                updateCounts[0] = execute( h, connection, statementHandle, -1 ).size();
            } catch ( Throwable e ) {
                log.error( "Exception while preparing query", e );
                String message = e.getLocalizedMessage();
                throw new AvaticaRuntimeException( message == null ? "null" : message, -1, "", AvaticaSeverity.ERROR );
            }

            return new ExecuteBatchResult( updateCounts );
        }
    }


    /**
     * Creates an iterable for a result set.
     *
     * The default implementation just returns {@code iterable}, which it requires to be not null; derived classes may instead choose to execute the relational
     * expression in {@code signature}.
     */
    @Override
    public Iterable<Object> createIterable( final StatementHandle h, final QueryState state, final Signature signature, final List<TypedValue> parameters, final Frame firstFrame ) {
        final PolyphenyDbConnectionHandle connection = openConnections.get( h.connectionId );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "createIterable( StatementHandle {}, QueryState {}, Signature {}, List<TypedValue> {}, Frame {} )", h, state, signature, parameters, firstFrame );
            }

            log.error( "[NOT IMPLEMENTED YET] createIterable( StatementHandle {}, QueryState {}, Signature {}, List<TypedValue> {}, Frame {} )", h, state, signature, parameters, firstFrame );
            return null;
        }
    }


    /**
     * Prepares a statement.
     *
     * @param ch Connection handle
     * @param sql SQL query
     * @param maxRowCount Negative for no limit (different meaning than JDBC)
     * @return Signature of prepared statement
     */
    @Override
    public StatementHandle prepare( final ConnectionHandle ch, final String sql, final long maxRowCount ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "prepare( ConnectionHandle {}, String {}, long {} )", ch, sql, maxRowCount );
            }

            StatementHandle h = createStatement( ch );
            PolyphenyDbStatementHandle polyphenyDbStatement;
            try {
                polyphenyDbStatement = getPolyphenyDbStatementHandle( h );
            } catch ( NoSuchStatementException e ) {
                throw new RuntimeException( e );
            }
            polyphenyDbStatement.setPreparedQuery( sql );

            Transaction transaction = connection.getCurrentOrCreateNewTransaction();
            Processor sqlProcessor = transaction.getProcessor( QueryLanguage.SQL );

            Node parsed = sqlProcessor.parse( sql ).get( 0 );
            // It is important not to add default values for missing fields in insert statements. If we would do this, the
            // JDBC driver would expect more parameter fields than there actually are in the query.
            Pair<Node, AlgDataType> validated = sqlProcessor.validate( transaction, parsed, false );
            AlgDataType parameterRowType = sqlProcessor.getParameterRowType( validated.left );

            List<AvaticaParameter> avaticaParameters = deriveAvaticaParameters( parameterRowType );

            PolyphenyDbSignature signature = new PolyphenyDbSignature<>(
                    sql,
                    avaticaParameters,
                    ImmutableMap.of(),
                    parameterRowType,
                    null,
                    null,
                    null,
                    ImmutableList.of(),
                    -1,
                    null,
                    StatementType.SELECT,
                    null,
                    NamespaceType.RELATIONAL );
            h.signature = signature;
            polyphenyDbStatement.setSignature( signature );

            return h;
        }
    }


    /**
     * Prepares and executes a statement.
     *
     * @param h Statement handle
     * @param sql SQL query
     * @param maxRowCount Negative for no limit (different meaning than JDBC)
     * @param callback Callback to lock, clear and assign cursor
     * @return Result containing statement ID, and if a query, a result set and first frame of data
     * @deprecated See {@link #prepareAndExecute(StatementHandle, String, long, int, PrepareCallback)}
     */
    @Override
    @Deprecated
    public ExecuteResult prepareAndExecute( final StatementHandle h, final String sql, final long maxRowCount, final PrepareCallback callback ) throws NoSuchStatementException {
        if ( log.isTraceEnabled() ) {
            log.trace( "prepareAndExecute( StatementHandle {}, String {}, long {}, PrepareCallback {} )", h, sql, maxRowCount, callback );
        }
        return prepareAndExecute( h, sql, maxRowCount, AvaticaUtils.toSaturatedInt( maxRowCount ), callback );
    }


    /**
     * Prepares and executes a statement.
     *
     * @param h Statement handle
     * @param sql SQL query
     * @param maxRowCount Maximum number of rows for the entire query. Negative for no limit (different meaning than JDBC).
     * @param maxRowsInFirstFrame Maximum number of rows for the first frame. This value should always be less than or equal to {@code maxRowCount} as the number of results are guaranteed to be restricted by {@code maxRowCount} and the underlying database.
     * @param callback Callback to lock, clear and assign cursor
     * @return Result containing statement ID, and if a query, a result set and first frame of data
     */
    @Override
    public ExecuteResult prepareAndExecute( final StatementHandle h, final String sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) throws NoSuchStatementException {
        final PolyphenyDbConnectionHandle connection = openConnections.get( h.connectionId );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "prepareAndExecute( StatementHandle {}, String {}, long {}, int {}, PrepareCallback {} )", h, sql, maxRowCount, maxRowsInFirstFrame, callback );
            }

            PolyphenyDbStatementHandle statementHandle = getPolyphenyDbStatementHandle( h );
            statementHandle.setPreparedQuery( sql );
            statementHandle.setStatement( connection.getCurrentOrCreateNewTransaction().createStatement() );
            return execute( h, new LinkedList<>(), maxRowsInFirstFrame, connection );
        }
    }


    /**
     * Prepares a statement and then executes a number of SQL commands in one pass.
     *
     * @param h Statement handle
     * @param sqlCommands SQL commands to run
     * @return An array of update counts containing one element for each command in the batch.
     */
    @Override
    public ExecuteBatchResult prepareAndExecuteBatch( final StatementHandle h, final List<String> sqlCommands ) throws NoSuchStatementException {
        final PolyphenyDbConnectionHandle connection = openConnections.get( h.connectionId );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "prepareAndExecuteBatch( StatementHandle {}, List<String> {} )", h, sqlCommands );
            }

            log.error( "[NOT IMPLEMENTED YET] prepareAndExecuteBatch( StatementHandle {}, List<String> {} )", h, sqlCommands );
            return null;
        }
    }


    /**
     * Executes a collection of bound parameter values on a prepared statement.
     *
     * @param h Statement handle
     * @param parameterValues A collection of list of typed values, one list per batch
     * @return An array of update counts containing one element for each command in the batch.
     */
    @Override
    public ExecuteBatchResult executeBatch( final StatementHandle h, final List<List<TypedValue>> parameterValues ) throws NoSuchStatementException {
        final PolyphenyDbConnectionHandle connection = openConnections.get( h.connectionId );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "executeBatch( StatementHandle {}, List<List<TypedValue>> {} )", h, parameterValues );
            }

            log.error( "[NOT IMPLEMENTED YET] executeBatch( StatementHandle {}, List<List<TypedValue>> {} )", h, parameterValues );
            return null;
        }
    }


    /**
     * Returns a frame of rows.
     *
     * The frame describes whether there may be another frame. If there is not another frame, the current iteration is done when we have finished the rows in the this frame.
     *
     * @param h Statement handle
     * @param offset Zero-based offset of first row in the requested frame
     * @param fetchMaxRowCount Maximum number of rows to return; negative means no limit
     * @return Frame, or null if there are no more
     */
    @Override
    public Frame fetch( final StatementHandle h, final long offset, final int fetchMaxRowCount ) throws NoSuchStatementException {
        final PolyphenyDbConnectionHandle connection = openConnections.get( h.connectionId );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "fetch( StatementHandle {}, long {}, int {} )", h, offset, fetchMaxRowCount );
            }

            final PolyphenyDbStatementHandle statementHandle = getPolyphenyDbStatementHandle( h );

            final PolyphenyDbSignature signature = statementHandle.getSignature();
            final Iterator<Object> iterator;
            if ( statementHandle.getOpenResultSet() == null ) {
                final Iterable<Object> iterable = createIterable( statementHandle.getStatement().getDataContext(), signature );
                iterator = iterable.iterator();
                statementHandle.setOpenResultSet( iterator );
                statementHandle.getExecutionStopWatch().start();
            } else {
                iterator = statementHandle.getOpenResultSet();
                statementHandle.getExecutionStopWatch().resume();
            }
            final List rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( iterator, fetchMaxRowCount ), new ArrayList<>() );
            statementHandle.getExecutionStopWatch().suspend();
            boolean done = fetchMaxRowCount == 0 || rows.size() < fetchMaxRowCount;
            @SuppressWarnings("unchecked")
            List<Object> rows1 = (List<Object>) rows;
            if ( done ) {
                statementHandle.getExecutionStopWatch().stop();
                signature.getExecutionTimeMonitor().setExecutionTime( statementHandle.getExecutionStopWatch().getNanoTime() );
                try {
                    if ( iterator instanceof AutoCloseable ) {
                        ((AutoCloseable) iterator).close();
                    }
                } catch ( Exception e ) {
                    log.error( "Exception while closing result iterator", e );
                }
            }
            return new Meta.Frame( offset, done, rows1 );
        }
    }


    private Iterable<Object> createIterable( DataContext dataContext, PolyphenyDbSignature signature ) {
        //noinspection unchecked
        final PolyphenyDbSignature<Object> polyphenyDbSignature = (PolyphenyDbSignature<Object>) signature;
        return polyphenyDbSignature.enumerable( dataContext );
    }


    /**
     * Executes a prepared statement.
     *
     * @param h Statement handle
     * @param parameterValues A list of parameter values; may be empty, not null
     * @param maxRowCount Maximum number of rows to return; negative means no limit
     * @return Execute result
     * @deprecated See {@link #execute(StatementHandle, List, int)}
     */
    @Override
    @Deprecated
    public ExecuteResult execute( final StatementHandle h, final List<TypedValue> parameterValues, final long maxRowCount ) throws NoSuchStatementException {
        if ( log.isTraceEnabled() ) {
            log.trace( "execute( StatementHandle {}, List<TypedValue> {}, long {} )", h, parameterValues, maxRowCount );
        }

        return execute( h, parameterValues, -1 );
    }


    /**
     * Executes a prepared statement.
     *
     * @param h Statement handle
     * @param parameterValues A list of parameter values; may be empty, not null
     * @param maxRowsInFirstFrame Maximum number of rows to return in the Frame.
     * @return Execute result
     */
    @Override
    public ExecuteResult execute( final StatementHandle h, final List<TypedValue> parameterValues, final int maxRowsInFirstFrame ) throws NoSuchStatementException {
        final PolyphenyDbConnectionHandle connection = openConnections.get( h.connectionId );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "execute( StatementHandle {}, List<TypedValue> {}, int {} )", h, parameterValues, maxRowsInFirstFrame );
            }
            final PolyphenyDbStatementHandle statementHandle = getPolyphenyDbStatementHandle( h );
            statementHandle.setStatement( connection.getCurrentOrCreateNewTransaction().createStatement() );
            return execute( h, parameterValues, maxRowsInFirstFrame, connection );
        }
    }


    private ExecuteResult execute( StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame, PolyphenyDbConnectionHandle connection ) throws NoSuchStatementException {
        final PolyphenyDbStatementHandle statementHandle = getPolyphenyDbStatementHandle( h );

        long index = 0;
        for ( TypedValue v : parameterValues ) {
            if ( v != null ) {
                Object o;
                if ( "ARRAY".equals( v.type.name() ) ) {
                    o = convertList( (List<Object>) v.toLocal() );
                } else {
                    o = v.toJdbc( calendar );
                }
                List<Object> list = new LinkedList<>();
                list.add( o );
                statementHandle.getStatement().getDataContext().addParameterValues( index++, null, list );
            }
        }

        try {
            prepare( h, statementHandle.getPreparedQuery() );
            return new ExecuteResult( execute( h, connection, statementHandle, maxRowsInFirstFrame ) );
        } catch ( Throwable e ) {
            log.error( "Exception while preparing query", e );
            String message = e.getLocalizedMessage();
            throw new AvaticaRuntimeException( message == null ? "null" : message, -1, "", AvaticaSeverity.ERROR );
        }
    }


    private List<Object> convertList( List<Object> list ) {
        List<Object> newList = new LinkedList<>();
        for ( Object o : list ) {
            if ( o instanceof List ) {
                newList.add( convertList( list ) );
            } else if ( o instanceof TypedValue ) {
                newList.add( ((TypedValue) o).toJdbc( calendar ) );
            }
        }
        return newList;
    }


    private void prepare( StatementHandle h, String sql ) throws NoSuchStatementException {
        PolyphenyDbStatementHandle statementHandle = getPolyphenyDbStatementHandle( h );
        Processor sqlProcessor = statementHandle.getStatement().getTransaction().getProcessor( QueryLanguage.SQL );

        Node parsed = sqlProcessor.parse( sql ).get( 0 );

        PolyphenyDbSignature<?> signature;
        if ( parsed.isA( Kind.DDL ) ) {
            signature = PolyphenyDbSignature.from( sqlProcessor.prepareDdl( statementHandle.getStatement(), parsed, new QueryParameters( sql, NamespaceType.RELATIONAL ) ) );
        } else {
            Pair<Node, AlgDataType> validated = sqlProcessor.validate(
                    statementHandle.getStatement().getTransaction(),
                    parsed,
                    RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() );
            AlgRoot logicalRoot = sqlProcessor.translate( statementHandle.getStatement(), validated.left, null );
            AlgDataType parameterRowType = sqlProcessor.getParameterRowType( validated.left );

            // Prepare
            signature = PolyphenyDbSignature.from( statementHandle.getStatement().getQueryProcessor().prepareQuery( logicalRoot, parameterRowType, true ) );
        }

        h.signature = signature;
        statementHandle.setSignature( signature );
    }


    private List<MetaResultSet> execute( StatementHandle h, PolyphenyDbConnectionHandle connection, PolyphenyDbStatementHandle statementHandle, int maxRowsInFirstFrame ) {
        List<MetaResultSet> resultSets;
        if ( statementHandle.getSignature().statementType == StatementType.OTHER_DDL ) {
            MetaResultSet resultSet = MetaResultSet.count( statementHandle.getConnection().getConnectionId().toString(), h.id, 1 );
            resultSets = ImmutableList.of( resultSet );
            commit( connection.getHandle() );
        } else if ( statementHandle.getSignature().statementType == StatementType.IS_DML ) {
            Iterator<?> iterator = statementHandle.getSignature().enumerable( statementHandle.getStatement().getDataContext() ).iterator();
            int rowsChanged = -1;
            try {
                rowsChanged = PolyImplementation.getRowsChanged( statementHandle.getStatement(), iterator, statementHandle.getStatement().getMonitoringEvent().getMonitoringType() );
            } catch ( Exception e ) {
                log.error( "Caught exception while retrieving row count", e );
            }

            MetaResultSet metaResultSet = MetaResultSet.count( h.connectionId, h.id, rowsChanged );
            resultSets = ImmutableList.of( metaResultSet );
        } else {
            try {
                resultSets = Collections.singletonList( MetaResultSet.create(
                        h.connectionId,
                        h.id,
                        false,
                        statementHandle.getSignature(),
                        // Due to a bug in Avatica (it wrongly replaces the courser type) I have per default disabled sending data with the first frame.
                        // TODO MV:  Due to the performance benefits of sending data together with the first frame, this issue should be addressed
                        //  Remember that fetch is synchronized
                        maxRowsInFirstFrame != 0 && SEND_FIRST_FRAME_WITH_RESPONSE
                                ? fetch( h, 0, (int) Math.min( Math.max( statementHandle.getMaxRowCount(), maxRowsInFirstFrame ), Integer.MAX_VALUE ) )
                                : null //Frame.MORE // Send first frame to together with the response to save a fetch call
                ) );
            } catch ( NoSuchStatementException e ) {
                String message = e.getLocalizedMessage();
                throw new AvaticaRuntimeException( message == null ? "null" : message, -1, "", AvaticaSeverity.ERROR );
            }
        }
        return resultSets;
    }


    /**
     * Called during the creation of a statement to allocate a new handle.
     *
     * @param ch Connection handle
     */
    @Override
    public StatementHandle createStatement( ConnectionHandle ch ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "createStatement( ConnectionHandle {} )", ch );
            }

            final PolyphenyDbStatementHandle statement;

            final int id = statementIdGenerator.getAndIncrement();
            statement = new PolyphenyDbStatementHandle( connection, id );
            openStatements.put( ch.id + "::" + id, statement );

            StatementHandle h = new StatementHandle( ch.id, statement.getStatementId(), null );
            log.trace( "created statement {}", h );
            return h;
        }
    }


    /**
     * Closes a statement.
     *
     * If the statement handle is not known, or is already closed, does nothing.
     *
     * @param statementHandle Statement handle
     */
    @Override
    public void closeStatement( final StatementHandle statementHandle ) {
        final PolyphenyDbConnectionHandle connection = openConnections.get( statementHandle.connectionId );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "closeStatement( StatementHandle {} )", statementHandle );
            }

            final PolyphenyDbStatementHandle toClose = openStatements.remove( statementHandle.connectionId + "::" + Integer.toString( statementHandle.id ) );
            if ( toClose != null ) {
                if ( toClose.getOpenResultSet() != null && toClose.getOpenResultSet() instanceof AutoCloseable ) {
                    try {
                        ((AutoCloseable) toClose.getOpenResultSet()).close();
                    } catch ( Exception e ) {
                        log.error( "Exception while closing result iterator", e );
                    }
                }
                toClose.unset();
            }
        }
    }


    /**
     * Opens (creates) a connection. The client allocates its own connection ID which the server is then made aware of through the {@link Meta.ConnectionHandle}.
     * The Map {@code info} argument is analogous to the {@link Properties} typically passed to a "normal" JDBC Driver. Avatica specific properties should not
     * be included -- only properties for the underlying driver.
     *
     * @param ch A ConnectionHandle encapsulates information about the connection to be opened as provided by the client.
     * @param connectionParameters A Map corresponding to the Properties typically passed to a JDBC Driver.
     */
    @Override
    public void openConnection( final ConnectionHandle ch, final Map<String, String> connectionParameters ) {
        if ( log.isTraceEnabled() ) {
            log.trace( "openConnection( ConnectionHandle {}, Map<String, String> {} )", ch, connectionParameters );
        }

        if ( openConnections.containsKey( ch.id ) ) {
            if ( log.isDebugEnabled() ) {
                log.debug( "Key {} is already present in the OPEN_CONNECTIONS map.", ch.id );
            }
            throw new IllegalStateException( "Forbidden attempt to open the connection `" + ch.id + "` twice!" );
        }

        if ( log.isDebugEnabled() ) {
            log.debug( "Creating a new connection." );
        }

        final CatalogUser user;
        try {
            user = authenticator.authenticate(
                    connectionParameters.getOrDefault( "username", connectionParameters.get( "user" ) ),
                    connectionParameters.getOrDefault( "password", "" ) );
        } catch ( AuthenticationException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }
        // assert user != null;

        String databaseName = connectionParameters.getOrDefault( "database", connectionParameters.get( "db" ) );
        if ( databaseName == null || databaseName.isEmpty() ) {
            databaseName = "APP";
        }
        String defaultSchemaName = connectionParameters.get( "schema" );
        if ( defaultSchemaName == null || defaultSchemaName.isEmpty() ) {
            defaultSchemaName = "public";
        }

        // Create transaction
        Transaction transaction = transactionManager.startTransaction( user, null, null, false, "AVATICA Interface" );

        // Check database access
        final CatalogDatabase database;
        try {
            database = catalog.getDatabase( databaseName );
        } catch ( UnknownDatabaseException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }
        assert database != null;

//            Authorizer.hasAccess( user, database );

        // Check schema access
        final CatalogSchema schema;
        try {
            schema = catalog.getSchema( database.id, defaultSchemaName );
        } catch ( UnknownSchemaException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }
        assert schema != null;

//            Authorizer.hasAccess( user, schema );

        // commit transaction
        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }

        openConnections.put( ch.id, new PolyphenyDbConnectionHandle( ch, user, ch.id, database, schema, transactionManager ) );
    }


    /**
     * Closes a connection
     */
    @Override
    public void closeConnection( ConnectionHandle ch ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "closeConnection( ConnectionHandle {} )", ch );
            }

            final PolyphenyDbConnectionHandle connectionToClose = openConnections.remove( ch.id );
            if ( connectionToClose == null ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "Connection {} already closed.", ch.id );
                }
                return;
            }

            // Check if there is a running transaction
            Transaction transaction = connectionToClose.getCurrentTransaction();
            if ( transaction != null && transaction.isActive() ) {
                log.warn( "There is a running transaction associated with this connection {}", connectionToClose );
                log.warn( "Rollback transaction {}", transaction );
                try {
                    transaction.rollback();
                } catch ( TransactionException e ) {
                    throw new RuntimeException( e );
                }
            }

            for ( final String key : openStatements.keySet() ) {
                if ( key.startsWith( ch.id ) ) {
                    PolyphenyDbStatementHandle statementHandle = openStatements.remove( key );
                    statementHandle.unset();
                }
            }

            // TODO: release all resources associated with this connection
        }
    }


    private PolyphenyDbConnectionHandle getPolyphenyDbConnectionHandle( String connectionId ) {
        if ( openConnections.containsKey( connectionId ) ) {
            return openConnections.get( connectionId );
        } else {
            throw new IllegalStateException( "Unknown connection id `" + connectionId + "`!" );
        }
    }


    private PolyphenyDbStatementHandle getPolyphenyDbStatementHandle( StatementHandle h ) throws NoSuchStatementException {
        final PolyphenyDbStatementHandle statement;
        if ( openStatements.containsKey( h.connectionId + "::" + Integer.toString( h.id ) ) ) {
            statement = openStatements.get( h.connectionId + "::" + Integer.toString( h.id ) );
        } else {
            throw new NoSuchStatementException( h );
        }
        return statement;
    }


    /**
     * Re-sets the {@link ResultSet} on a Statement. Not a JDBC method.
     *
     * @return True if there are results to fetch after resetting to the given offset. False otherwise
     */
    @Override
    public boolean syncResults( final StatementHandle sh, final QueryState state, final long offset ) throws NoSuchStatementException {
        if ( log.isTraceEnabled() ) {
            log.trace( "syncResults( StatementHandle {}, QueryState {}, long {} )", sh, state, offset );
        }

        log.error( "[NOT IMPLEMENTED YET] syncResults( StatementHandle {}, QueryState {}, long {} )", sh, state, offset );
        return false;
    }


    /**
     * Makes all changes since the last commit/rollback permanent.
     */
    @Override
    public void commit( final ConnectionHandle ch ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "commit( ConnectionHandle {} )", ch );
            }
            Transaction transaction = connection.getCurrentTransaction();

            if ( transaction == null || !transaction.isActive() ) {
                if ( log.isTraceEnabled() ) {
                    log.trace( "No open transaction for ConnectionHandle {}", connection );
                }
                return;
            }

            try {
                transaction.commit();
            } catch ( TransactionException e ) {
                throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
            } finally {
                connection.endCurrentTransaction();
            }
        }
    }


    /**
     * Undoes all changes since the last commit/rollback.
     */
    @Override
    public void rollback( final ConnectionHandle ch ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "rollback( ConnectionHandle {} )", ch );
            }
            Transaction transaction = connection.getCurrentTransaction();

            if ( transaction == null || !transaction.isActive() ) {
                if ( log.isTraceEnabled() ) {
                    log.trace( "No open transaction for ConnectionHandle {}", connection );
                }
                return;
            }

            try {
                transaction.rollback();
            } catch ( TransactionException e ) {
                throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
            } finally {
                connection.endCurrentTransaction();
            }
        }
    }


    /**
     * Synchronizes client and server view of connection properties.
     *
     * Note: this interface is considered "experimental" and may undergo further changes as this functionality is extended to other aspects of state management for
     * {@link Connection}, {@link Statement}, and {@link ResultSet}.
     */
    @Override
    public ConnectionProperties connectionSync( ConnectionHandle ch, ConnectionProperties connProps ) {
        final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "connectionSync( ConnectionHandle {}, ConnectionProperties {} )", ch, connProps );
            }

            final PolyphenyDbConnectionHandle connectionToSync = openConnections.get( ch.id );
            if ( connectionToSync == null ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "Connection {} is not open.", ch.id );
                }
                throw new IllegalStateException( "Attempt to synchronize the connection `" + ch.id + "` with is either has not been open yet or is already closed." );
            }

            return connection.mergeConnectionProperties( connProps );
        }
    }


    private RuntimeException propagate( Throwable e ) {
        if ( e instanceof RuntimeException ) {
            throw (RuntimeException) e;
        } else if ( e instanceof Error ) {
            throw (Error) e;
        } else {
            throw new RuntimeException( e );
        }
    }


    public List<AvaticaParameter> deriveAvaticaParameters( AlgDataType parameterRowType ) {
        final List<AvaticaParameter> parameters = new ArrayList<>();
        for ( AlgDataTypeField field : parameterRowType.getFieldList() ) {
            AlgDataType type = field.getType();
            parameters.add(
                    new AvaticaParameter(
                            false,
                            type.getPrecision() == AlgDataType.PRECISION_NOT_SPECIFIED ? 0 : type.getPrecision(),
                            type.getScale() == AlgDataType.SCALE_NOT_SPECIFIED ? 0 : type.getScale(),
                            type.getPolyType().getJdbcOrdinal(),
                            type.getPolyType().getTypeName(),
                            Object.class.getName(),
                            field.getName() ) );
        }
        return parameters;
    }


    private class MonitoringPage {

        private final InformationPage informationPage;
        private final InformationGroup informationGroupConnectionStatistics;
        private final InformationTable connectionNumberTable;

        private final InformationGroup informationGroupConnectionList;
        private final InformationTable connectionListTable;


        public MonitoringPage( String uniqueName ) {
            InformationManager im = InformationManager.getInstance();

            informationPage = new InformationPage( uniqueName, "AVATICA Interface" ).fullWidth().setLabel( "Interfaces" );
            informationGroupConnectionStatistics = new InformationGroup( informationPage, "Connection Statistics" );

            im.addPage( informationPage );
            im.addGroup( informationGroupConnectionStatistics );

            //// connectionNumberTable
            connectionNumberTable = new InformationTable(
                    informationGroupConnectionStatistics,
                    Arrays.asList( "Attribute", "Value" ) );
            connectionNumberTable.setOrder( 1 );
            im.registerInformation( connectionNumberTable );
            //

            //// connectionListTable
            informationGroupConnectionList = new InformationGroup( informationPage, "Connections" );
            im.addGroup( informationGroupConnectionList );
            connectionListTable = new InformationTable(
                    informationGroupConnectionList,
                    Arrays.asList( "Connection ID", "User", "Driver", "TX", "Auto Commit", "Client IP" ) );
            connectionListTable.setOrder( 2 );
            im.registerInformation( connectionListTable );
            //

            informationPage.setRefreshFunction( this::update );
        }


        public void updateConnectionNumberTable() {
            connectionNumberTable.reset();
            connectionNumberTable.addRow( "Open Statements", "" + openStatements.size() );
            connectionNumberTable.addRow( "Open Connections", "" + openStatements.size() );
        }


        public void updateConnectionListTable() {
            connectionListTable.reset();

            for ( Entry<String, PolyphenyDbConnectionHandle> entry : openConnections.entrySet() ) {
                String connectionId = entry.getKey();
                PolyphenyDbConnectionHandle connectionHandle = entry.getValue();
                Transaction currentTx = connectionHandle.getCurrentTransaction();
                String txId = "-";
                if ( currentTx != null ) {
                    txId = String.valueOf( currentTx.getId() );
                }

                connectionListTable.addRow(
                        connectionId,
                        connectionHandle.getUser().name,
                        "N/A",
                        txId,
                        connectionHandle.isAutoCommit(),
                        "N/A"
                );
            }
        }


        public void update() {
            updateConnectionNumberTable();
            updateConnectionListTable();
        }


        public void remove() {
            InformationManager im = InformationManager.getInstance();
            im.removeInformation( connectionNumberTable );
            im.removeInformation( connectionListTable );
            im.removeGroup( informationGroupConnectionStatistics );
            im.removePage( informationPage );
        }

    }

}