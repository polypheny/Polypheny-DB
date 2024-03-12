/*
 * Copyright 2019-2024 The Polypheny Project
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
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
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
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MetaImpl.MetaTypeInfo;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.proto.Common;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.avatica.remote.ProtobufMeta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.Unsafe;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.catalog.entity.PolyObject;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalColumn.PrimitiveCatalogColumn;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey.LogicalForeignKeyField;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey.LogicalForeignKeyField.PrimitiveCatalogForeignKeyColumn;
import org.polypheny.db.catalog.entity.logical.LogicalIndex;
import org.polypheny.db.catalog.entity.logical.LogicalIndex.LogicalIndexField;
import org.polypheny.db.catalog.entity.logical.LogicalIndex.LogicalIndexField.PrimitiveCatalogIndexColumn;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace.PrimitiveCatalogSchema;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey.LogicalPrimaryKeyField;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey.LogicalPrimaryKeyField.PrimitiveCatalogPrimaryKeyColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.logical.LogicalTable.PrimitiveCatalogTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.EntityType.PrimitiveTableType;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.functions.TemporalFunctions;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyLong;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.LimitIterator;
import org.polypheny.db.util.Pair;


@Slf4j
public class DbmsMeta implements ProtobufMeta {

    public static final boolean SEND_FIRST_FRAME_WITH_RESPONSE = false;
    public static final JavaTypeFactoryImpl TYPE_FACTORY = new JavaTypeFactoryImpl();

    private final ConcurrentMap<String, PolyConnectionHandle> openConnections = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, PolyStatementHandle<Object>> openStatements = new ConcurrentHashMap<>();

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
                throw new GenericRuntimeException( e );
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
            final Iterable<PolyValue[]> firstFrame ) {
        final PolySignature signature =
                new PolySignature(
                        "",
                        ImmutableList.of(),
                        internalParameters,
                        null,
                        columns,
                        CursorFactory.LIST,
                        ImmutableList.of(),
                        -1,
                        null,
                        StatementType.SELECT,
                        new ExecutionTimeMonitor(),
                        DataModel.RELATIONAL ) {
                    @Override
                    public Enumerable<PolyValue[]> enumerable( DataContext dataContext ) {
                        return Linq4j.asEnumerable( firstFrame );
                    }
                };
        // changed with th branch
        List<Object> list = StreamSupport.stream( firstFrame.spliterator(), false ).map( vs -> (Object) Arrays.stream( vs ).map( v -> v == null ? null : v.toJava() ).toArray( Object[]::new ) ).toList();
        return MetaResultSet.create( ch.id, statementHandle.id, true, signature, Meta.Frame.create( 0, false, list ) );
    }


    private MetaResultSet createMetaResultSet( final ConnectionHandle ch, final StatementHandle statementHandle, Enumerable<PolyValue[]> enumerable, Class<?> clazz, String... names ) {
        final List<ColumnMetaData> columns = new ArrayList<>();

        int i = 0;
        for ( String name : names ) {
            final String fieldName = AvaticaUtils.toCamelCase( name );
            final Class<?> type;
            try {
                type = clazz.isRecord() ? clazz.getMethod( fieldName ).getReturnType() : clazz.getField( fieldName ).getType();
            } catch ( NoSuchFieldException | NoSuchMethodException e ) {
                throw new GenericRuntimeException( e );
            }
            columns.add( MetaImpl.columnMetaData( name, i, type, false ) );
            i++;
        }

        return createMetaResultSet( ch, statementHandle, new HashMap<>(), columns, enumerable );
    }


    private Enumerable<PolyValue[]> toEnumerable( final List<? extends PolyObject> entities ) {
        final List<PolyValue[]> objects = new ArrayList<>();
        for ( PolyObject entity : entities ) {
            objects.add( entity.getParameterArray() );
        }
        return Linq4j.asEnumerable( objects );
    }


    /**
     * Returns a map of static database properties.
     * <p>
     * The provider can omit properties whose value is the same as the default.
     */
    @Override
    public Map<DatabaseProperty, Object> getDatabaseProperties( ConnectionHandle ch ) {
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getTables( ConnectionHandle {}, String {}, Pat {}, Pat {}, List<String> {} )", ch, database, schemaPattern, tablePattern, typeList );
            }

            final List<LogicalTable> tables = getLogicalTables( schemaPattern, tablePattern );
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


    @Nonnull
    private List<LogicalTable> getLogicalTables( Pat schemaPattern, Pat tablePattern ) {
        return getLogicalTables( (schemaPattern == null || schemaPattern.s == null) ? null : new Pattern( schemaPattern.s ),
                (tablePattern == null || tablePattern.s == null) ? null : new Pattern( tablePattern.s ) );
    }


    @Nonnull
    private List<LogicalTable> getLogicalTables( Pattern schemaPattern, Pattern tablePattern ) {
        //List<LogicalNamespace> namespaces = catalog.getSnapshot().getNamespaces( schemaPattern );

        return catalog.getSnapshot().rel().getTables( schemaPattern, tablePattern );
    }


    @Override
    public MetaResultSet getColumns( final ConnectionHandle ch, final String database, final Pat schemaPattern, final Pat tablePattern, final Pat columnPattern ) {
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getAllocColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, database, schemaPattern, tablePattern, columnPattern );
            }
            final List<LogicalColumn> columns = getLogicalTables( schemaPattern, tablePattern ).stream().flatMap( t -> catalog.getSnapshot().rel().getColumns(
                    (tablePattern == null || tablePattern.s == null) ? null : new Pattern( tablePattern.s ),
                    (columnPattern == null || columnPattern.s == null) ? null : new Pattern( columnPattern.s )
            ).stream() ).toList();
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getNamespaces( ConnectionHandle {}, String {}, Pat {} )", ch, database, schemaPattern );
            }
            final List<LogicalNamespace> schemas = catalog.getSnapshot().getNamespaces(
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getCatalogs( ConnectionHandle {} )", ch );
            }
            //final List<CatalogDatabase> databases = Linq4j.asEnumerable( new String[]{ "APP", "system", "public" } );
            List<PolyValue[]> databases = Collections.singletonList( new PolyValue[]{ PolyString.of( Catalog.DATABASE_NAME ), PolyString.of( "system" ), PolyString.of( Catalog.DEFAULT_NAMESPACE_NAME ) } );
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    Linq4j.asEnumerable( databases ),
                    PrimitiveDatabase.class,
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getTableTypes( ConnectionHandle {} )", ch );
            }
            final List<PolyValue[]> objects = new LinkedList<>();
            for ( EntityType tt : EntityType.values() ) {
                objects.add( tt.getParameterArray() );
            }
            Enumerable<PolyValue[]> enumerable = Linq4j.asEnumerable( objects );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getPrimaryKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, database, schema, table );
            }
            final Pattern tablePattern = table == null ? null : new Pattern( table );
            final Pattern schemaPattern = schema == null ? null : new Pattern( schema );
            final List<LogicalTable> catalogEntities = getLogicalTables( schemaPattern, tablePattern );
            List<LogicalPrimaryKeyField> primaryKeyColumns = new LinkedList<>();
            for ( LogicalTable catalogTable : catalogEntities ) {
                if ( catalogTable.primaryKey != null ) {
                    final LogicalPrimaryKey primaryKey = catalog.getSnapshot().rel().getPrimaryKey( catalogTable.primaryKey ).orElseThrow();
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getImportedKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, database, schema, table );
            }
            final Pattern tablePattern = table == null ? null : new Pattern( table );
            final Pattern schemaPattern = schema == null ? null : new Pattern( schema );
            final List<LogicalTable> catalogEntities = getLogicalTables( schemaPattern, tablePattern );
            List<LogicalForeignKeyField> foreignKeyColumns = new LinkedList<>();
            for ( LogicalTable catalogTable : catalogEntities ) {
                List<LogicalForeignKey> importedKeys = catalog.getSnapshot().rel().getForeignKeys( catalogTable.id );
                importedKeys.forEach( catalogForeignKey -> foreignKeyColumns.addAll( catalogForeignKey.getCatalogForeignKeyFields() ) );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getExportedKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, database, schema, table );
            }
            final Pattern tablePattern = table == null ? null : new Pattern( table );
            final Pattern schemaPattern = schema == null ? null : new Pattern( schema );

            final List<LogicalTable> catalogEntities = getLogicalTables( schemaPattern, tablePattern );
            List<LogicalForeignKeyField> foreignKeyColumns = new LinkedList<>();
            for ( LogicalTable catalogTable : catalogEntities ) {
                List<LogicalForeignKey> exportedKeys = catalog.getSnapshot().rel().getExportedKeys( catalogTable.id );
                exportedKeys.forEach( catalogForeignKey -> foreignKeyColumns.addAll( catalogForeignKey.getCatalogForeignKeyFields() ) );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getTypeInfo( ConnectionHandle {} )", ch );
            }
            final StatementHandle statementHandle = createStatement( ch );
            final AlgDataTypeSystem typeSystem = AlgDataTypeSystem.DEFAULT;
            final List<PolyValue[]> objects = new ArrayList<>();
            for ( PolyType polyType : PolyType.values() ) {
                objects.add(
                        new PolyValue[]{
                                PolyString.of( polyType.getName() ),
                                PolyInteger.of( polyType.getJdbcOrdinal() ),
                                PolyInteger.of( typeSystem.getMaxPrecision( polyType ) ),
                                PolyString.of( typeSystem.getLiteral( polyType, true ) ),
                                PolyString.of( typeSystem.getLiteral( polyType, false ) ),
                                null,
                                PolyInteger.of( DatabaseMetaData.typeNullable ), // All types are nullable
                                PolyBoolean.of( typeSystem.isCaseSensitive( polyType ) ),
                                PolyInteger.of( DatabaseMetaData.typeSearchable ), // Making all type searchable; we may want to be specific and declare under PolyType
                                PolyBoolean.FALSE,
                                PolyBoolean.FALSE,
                                PolyBoolean.of( typeSystem.isAutoincrement( polyType ) ),
                                PolyString.of( polyType.getName() ),
                                PolyInteger.of( polyType.getMinScale() ),
                                PolyInteger.of( typeSystem.getMaxScale( polyType ) ),
                                null,
                                null,
                                PolyInteger.of( typeSystem.getNumTypeRadix( polyType ) == 0 ? null : typeSystem.getNumTypeRadix( polyType ) ) } );
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
                    "CASE_SENSITIVE",      // Indicates if the data type is case-sensitive. "true" if the type is case-sensitive; otherwise, "false".
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
    public MetaResultSet getIndexInfo( final ConnectionHandle ch, final String database, final String namespace, final String table, final boolean unique, final boolean approximate ) {
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "getIndexInfo( ConnectionHandle {}, String {}, String {}, String {}, boolean {}, boolean {} )", ch, database, namespace, table, unique, approximate );
            }
            final Pattern tablePattern = table == null ? null : new Pattern( table );
            final Pattern namespacePattern = namespace == null ? null : new Pattern( namespace );
            final List<LogicalTable> entities = getLogicalTables( namespacePattern, tablePattern );
            List<LogicalIndexField> logicalIndexFields = new ArrayList<>();
            for ( LogicalTable entity : entities ) {
                List<LogicalIndex> logicalIndexInfos = catalog.getSnapshot().rel().getIndexes( entity.id, unique );
                logicalIndexInfos.forEach( info -> logicalIndexFields.addAll( info.getIndexFields() ) );
            }
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( logicalIndexFields ),
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
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
        final PolyConnectionHandle connection = openConnections.get( h.connectionId );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "executeBatchProtobuf( StatementHandle {}, List<UpdateBatch> {} )", h, parameterValues );
            }

            final PolyStatementHandle<?> statementHandle = getPolyphenyDbStatementHandle( h );

            long[] updateCounts = new long[parameterValues.size()];
            Map<Long, List<PolyValue>> values = new HashMap<>();
            List<AlgDataType> types = new ArrayList<>();
            for ( UpdateBatch updateBatch : parameterValues ) {
                List<Common.TypedValue> list = updateBatch.getParameterValuesList();
                long index = 0;
                for ( Common.TypedValue v : list ) {
                    long i = index++;
                    if ( !values.containsKey( i ) ) {
                        values.put( i, new ArrayList<>() );
                    }
                    if ( v.getType() == Common.Rep.ARRAY ) {
                        values.get( i ).add( convertList( (List<TypedValue>) TypedValue.fromProto( v ).toLocal() ) );
                    } else {
                        values.get( i ).add( toPolyValue( TypedValue.fromProto( v ) ) );
                    }
                    types.add( (int) i, toPolyAlgType( TypedValue.fromProto( v ), TYPE_FACTORY ) );
                }
            }

            try {
                if ( values.isEmpty() ) {
                    // Nothing to execute
                    return new ExecuteBatchResult( new long[0] );
                }
                statementHandle.setStatement( connection.getCurrentOrCreateNewTransaction().createStatement() );
                int i = 0;
                for ( Entry<Long, List<PolyValue>> valuesList : values.entrySet() ) {
                    statementHandle.getStatement().getDataContext().addParameterValues( valuesList.getKey(), types.get( i++ ), valuesList.getValue() );
                }
                prepare( h, statementHandle.getPreparedQuery() );
                updateCounts[0] = execute( h, connection, statementHandle, -1 ).size();
            } catch ( Throwable e ) {
                log.error( "Exception while preparing query", e );
                String message = e.getLocalizedMessage();
                throw new GenericRuntimeException( message == null ? "null" : message, -1, "", AvaticaSeverity.ERROR );
            }

            return new ExecuteBatchResult( updateCounts );
        }
    }


    /**
     * Creates an iterable for a result set.
     * <p>
     * The default implementation just returns {@code iterable}, which it requires to be not null; derived classes may instead choose to execute the algebra
     * expression in {@code signature}.
     */
    @Override
    public Iterable<Object> createIterable( final StatementHandle h, final QueryState state, final Signature signature, final List<TypedValue> parameters, final Frame firstFrame ) {
        final PolyConnectionHandle connection = openConnections.get( h.connectionId );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "createExternalIterable( StatementHandle {}, QueryState {}, Signature {}, List<TypedValue> {}, Frame {} )", h, state, signature, parameters, firstFrame );
            }

            log.error( "[NOT IMPLEMENTED YET] createExternalIterable( StatementHandle {}, QueryState {}, Signature {}, List<TypedValue> {}, Frame {} )", h, state, signature, parameters, firstFrame );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "prepare( ConnectionHandle {}, String {}, long {} )", ch, sql, maxRowCount );
            }

            StatementHandle h = createStatement( ch );
            PolyStatementHandle<?> polyphenyDbStatement;
            try {
                polyphenyDbStatement = getPolyphenyDbStatementHandle( h );
            } catch ( NoSuchStatementException e ) {
                throw new GenericRuntimeException( e );
            }
            polyphenyDbStatement.setPreparedQuery( sql );

            Transaction transaction = connection.getCurrentOrCreateNewTransaction();
            Processor sqlProcessor = transaction.getProcessor( QueryLanguage.from( "sql" ) );

            Node parsed = sqlProcessor.parse( sql ).get( 0 );
            // It is important not to add default values for missing fields in insert statements. If we would do this, the
            // JDBC driver would expect more parameter fields than there actually are in the query.
            Pair<Node, AlgDataType> validated = sqlProcessor.validate( transaction, parsed, false );
            AlgDataType parameterRowType = sqlProcessor.getParameterRowType( validated.left );

            List<AvaticaParameter> avaticaParameters = deriveAvaticaParameters( parameterRowType );

            PolySignature signature = new PolySignature(
                    sql,
                    avaticaParameters,
                    ImmutableMap.of(),
                    parameterRowType,
                    null,
                    null,
                    ImmutableList.of(),
                    -1,
                    null,
                    StatementType.SELECT,
                    null,
                    DataModel.RELATIONAL );
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
        final PolyConnectionHandle connection = openConnections.get( h.connectionId );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "prepareAndExecute( StatementHandle {}, String {}, long {}, int {}, PrepareCallback {} )", h, sql, maxRowCount, maxRowsInFirstFrame, callback );
            }

            PolyStatementHandle<?> statementHandle = getPolyphenyDbStatementHandle( h );
            statementHandle.setPreparedQuery( sql );
            statementHandle.setStatement( connection.getCurrentOrCreateNewTransaction().createStatement() );
            return execute( h, new ArrayList<>(), maxRowsInFirstFrame, connection );
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
        final PolyConnectionHandle connection = openConnections.get( h.connectionId );
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
        final PolyConnectionHandle connection = openConnections.get( h.connectionId );
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
     * <p>
     * The frame describes whether there may be another frame. If there is not another frame, the current iteration is done when we have finished the rows in the this frame.
     *
     * @param h Statement handle
     * @param offset Zero-based offset of first row in the requested frame
     * @param fetchMaxRowCount Maximum number of rows to return; negative means no limit
     * @return Frame, or null if there are no more
     */
    @Override
    public Frame fetch( final StatementHandle h, final long offset, final int fetchMaxRowCount ) throws NoSuchStatementException {
        final PolyConnectionHandle connection = openConnections.get( h.connectionId );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "fetch( StatementHandle {}, long {}, int {} )", h, offset, fetchMaxRowCount );
            }

            final PolyStatementHandle<Object> statementHandle = getPolyphenyDbStatementHandle( h );

            final PolySignature signature = statementHandle.getSignature();
            final Iterator<Object> iterator;
            if ( statementHandle.getStatement() == null ) {
                // this is a hotfix for catalog methods of dbmsmeta todo dl diff in jdbc bench
                return new Frame( 0, true, new ArrayList<>() );
            }
            if ( statementHandle.getOpenResultSet() == null ) {
                final Iterable<Object> iterable = createExternalIterable( statementHandle.getStatement().getDataContext(), signature );
                iterator = iterable.iterator();
                statementHandle.setOpenResultSet( iterator );
                statementHandle.getExecutionStopWatch().start();
            } else {
                iterator = statementHandle.getOpenResultSet();
                statementHandle.getExecutionStopWatch().resume();
            }

            final List<?> rows = MetaImpl.collect( signature.cursorFactory, LimitIterator.of( iterator, fetchMaxRowCount ), new ArrayList<>() );
            statementHandle.getExecutionStopWatch().suspend();
            boolean done = fetchMaxRowCount == 0 || rows.size() < fetchMaxRowCount;

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
            return new Meta.Frame( offset, done, (Iterable<Object>) rows );
        }
    }


    private Iterable<Object> createExternalIterable( DataContext dataContext, PolySignature signature ) {
        return externalize( signature.enumerable( dataContext ), signature.rowType );
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
        final PolyConnectionHandle connection = openConnections.get( h.connectionId );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "execute( StatementHandle {}, List<TypedValue> {}, int {} )", h, parameterValues, maxRowsInFirstFrame );
            }
            final PolyStatementHandle<?> statementHandle = getPolyphenyDbStatementHandle( h );
            statementHandle.setStatement( connection.getCurrentOrCreateNewTransaction().createStatement() );
            return execute( h, parameterValues, maxRowsInFirstFrame, connection );
        }
    }


    private ExecuteResult execute( StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame, PolyConnectionHandle connection ) throws NoSuchStatementException {
        final PolyStatementHandle<?> statementHandle = getPolyphenyDbStatementHandle( h );

        long index = 0;
        for ( TypedValue v : parameterValues ) {
            if ( v != null ) {
                statementHandle.getStatement().getDataContext().addParameterValues( index++, toPolyAlgType( v, connection.getCurrentTransaction().getTypeFactory() ), List.of( toPolyValue( v ) ) );
            }
        }

        try {
            prepare( h, statementHandle.getPreparedQuery() );
            List<MetaResultSet> results = execute( h, connection, statementHandle, maxRowsInFirstFrame );
            if ( List.of( StatementType.OTHER_DDL, StatementType.OTHER_DDL, StatementType.IS_DML ).contains( statementHandle.getSignature().statementType ) && connection.isAutoCommit() ) {
                try {
                    commit( connection.getHandle() );
                } catch ( Exception e ) {
                    rollback( connection.getHandle() );

                    throw new GenericRuntimeException( "Error on auto-commit, transaction was rolled back.\n\n" + e );
                }
            }

            return new ExecuteResult( results );
        } catch ( Throwable e ) {
            log.error( "Exception while preparing query", e );
            String message = e.getLocalizedMessage();
            throw new GenericRuntimeException( message == null ? "null" : message, -1, "", AvaticaSeverity.ERROR );
        }
    }


    private AlgDataType toPolyAlgType( TypedValue value, JavaTypeFactory typeFactory ) {
        if ( value.value == null ) {
            return typeFactory.createPolyType( PolyType.NULL );
        }
        PolyType type = toPolyType( value.type );

        if ( type == PolyType.ARRAY ) {
            return typeFactory.createArrayType( typeFactory.createPolyType( toPolyType( value.componentType ) ), -1 );
        }

        return typeFactory.createPolyType( type );
    }


    private PolyType toPolyType( Rep type ) {
        if ( type == Rep.ARRAY ) {
            return PolyType.ARRAY;
        }
        switch ( type ) {
            case SHORT:
            case BYTE:
                return PolyType.TINYINT; // cache this
            case LONG:
                return PolyType.BIGINT;
            case NUMBER:
                return PolyType.DECIMAL;
            case JAVA_SQL_TIME:
                return PolyType.TIME;
            case JAVA_SQL_DATE:
                return PolyType.DATE;
            case JAVA_SQL_TIMESTAMP:
                return PolyType.TIMESTAMP;
            case BOOLEAN:
                return PolyType.BOOLEAN;
            case DOUBLE:
                return PolyType.DOUBLE;
            case INTEGER:
                return PolyType.INTEGER;
            case FLOAT:
                return PolyType.FLOAT;
            case STRING:
                return PolyType.VARCHAR;
            case BYTE_STRING:
                return PolyType.BINARY;
            case OBJECT:
                return PolyType.OTHER;
        }
        throw new NotImplementedException( "sql to polyType " + type );
    }


    private PolyList<PolyValue> convertList( List<TypedValue> list ) {
        List<PolyValue> newList = new ArrayList<>();
        for ( TypedValue o : list ) {
            newList.add( toPolyValue( o ) );
        }
        return PolyList.of( newList );
    }


    private PolyValue toPolyValue( TypedValue value ) {
        if ( value.type == Rep.ARRAY ) {
            return convertList( (List<TypedValue>) value.toLocal() );
        }
        if ( value.value == null ) {
            return PolyNull.NULL;
        }

        Object jdbc = value.toJdbc( calendar );
        return switch ( value.type ) {
            case FLOAT -> PolyFloat.of( (Number) jdbc );
            case INTEGER -> PolyInteger.of( (Number) jdbc );
            case LONG -> PolyLong.of( (Number) jdbc );
            case STRING -> PolyString.of( (String) jdbc );
            case BOOLEAN -> PolyBoolean.of( (Boolean) jdbc );
            case JAVA_SQL_DATE -> PolyDate.of( (Date) jdbc );
            case JAVA_SQL_TIME -> PolyTime.of( (Time) jdbc );
            case JAVA_SQL_TIMESTAMP -> PolyTimestamp.of( ((Timestamp) jdbc).getTime() + TemporalFunctions.LOCAL_TZ.getRawOffset() );
            case NUMBER -> PolyBigDecimal.of( (BigDecimal) jdbc );
            case DOUBLE -> PolyDouble.of( (Double) jdbc );
            case SHORT -> PolyInteger.of( (Short) jdbc );
            case BYTE -> PolyInteger.of( (byte) jdbc );
            case BYTE_STRING -> PolyBinary.of( (byte[]) jdbc );
            default -> throw new NotImplementedException( "dbms to poly " + value.type );
        };
    }


    private void prepare( StatementHandle h, String sql ) throws NoSuchStatementException {
        PolyStatementHandle<?> statementHandle = getPolyphenyDbStatementHandle( h );
        QueryLanguage language = QueryLanguage.from( "sql" );
        QueryContext context = QueryContext.builder()
                .query( sql )
                .language( language )
                .origin( "DBMS Meta" )
                .transactionManager( transactionManager )
                .build();

        PolySignature signature = PolySignature.from( LanguageManager.getINSTANCE().anyPrepareQuery( context, statementHandle.getStatement() ).get( 0 ) );

        h.signature = signature;
        statementHandle.setSignature( signature );
    }


    private Enumerable<Object> externalize( Enumerable<PolyValue[]> enumerable, AlgDataType rowType ) {
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object> enumerator() {
                List<Function1<PolyValue, Object>> transform = new ArrayList<>();
                for ( AlgDataTypeField field : rowType.getFields() ) {
                    transform.add( PolyValue.wrapNullableIfNecessary( PolyValue.getPolyToJava( field.getType(), true ), field.getType().isNullable() ) );
                }
                boolean isSingle = rowType.getFieldCount() == 1;

                return Linq4j.transform( enumerable.enumerator(), row -> {
                    if ( isSingle ) {
                        return transform.get( 0 ).apply( row[0] );
                    }

                    Object[] objects = new Object[row.length];
                    for ( int i = 0, rowLength = objects.length; i < rowLength; i++ ) {
                        objects[i] = transform.get( i ).apply( row[i] );
                    }
                    return objects;
                } );
            }
        };
    }


    private List<MetaResultSet> execute( StatementHandle h, PolyConnectionHandle connection, PolyStatementHandle<?> statementHandle, int maxRowsInFirstFrame ) {
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
                                ? fetch( h, 0, Math.max( statementHandle.getMaxRowCount(), maxRowsInFirstFrame ) )
                                : null //Frame.MORE // Send first frame to together with the response to save a fetch call
                ) );
            } catch ( NoSuchStatementException e ) {
                String message = e.getLocalizedMessage();
                throw new GenericRuntimeException( message == null ? "null" : message, -1, "", AvaticaSeverity.ERROR );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "createStatement( ConnectionHandle {} )", ch );
            }

            final PolyStatementHandle<Object> statement;

            final int id = statementIdGenerator.getAndIncrement();
            statement = new PolyStatementHandle<>( connection, id );
            openStatements.put( ch.id + "::" + id, statement );

            StatementHandle h = new StatementHandle( ch.id, statement.getStatementId(), null );
            log.trace( "created statement {}", h );
            return h;
        }
    }


    /**
     * Closes a statement.
     * <p>
     * If the statement handle is not known, or is already closed, does nothing.
     *
     * @param statementHandle Statement handle
     */
    @Override
    public void closeStatement( final StatementHandle statementHandle ) {
        final PolyConnectionHandle connection = openConnections.get( statementHandle.connectionId );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "closeStatement( StatementHandle {} )", statementHandle );
            }

            final PolyStatementHandle<?> toClose = openStatements.remove( statementHandle.connectionId + "::" + statementHandle.id );
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
            throw new IllegalStateException( "Forbidden attempt to execute the connection `" + ch.id + "` twice!" );
        }

        if ( log.isDebugEnabled() ) {
            log.debug( "Creating a new connection." );
        }

        final LogicalUser user;
        try {
            user = authenticator.authenticate(
                    connectionParameters.getOrDefault( "username", connectionParameters.get( "user" ) ),
                    connectionParameters.getOrDefault( "password", "" ) );
        } catch ( AuthenticationException e ) {
            throw new GenericRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }
        // assert user != null;

        String databaseName = connectionParameters.getOrDefault( "database", connectionParameters.get( "db" ) );
        if ( databaseName == null || databaseName.isEmpty() ) {
            databaseName = Catalog.DATABASE_NAME;
        }
        String defaultSchemaName = connectionParameters.get( "schema" );
        if ( defaultSchemaName == null || defaultSchemaName.isEmpty() ) {
            defaultSchemaName = Catalog.DEFAULT_NAMESPACE_NAME;
        }

        // Create transaction
        Transaction transaction = transactionManager.startTransaction( user.id, false, "AVATICA Interface" );

        //Authorizer.hasAccess( user, database );

        // Check schema access
        final LogicalNamespace namespace = catalog.getSnapshot().getNamespace( defaultSchemaName ).orElseThrow();

        //Authorizer.hasAccess( user, schema );

        // commit transaction
        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            throw new GenericRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }

        openConnections.put( ch.id, new PolyConnectionHandle( ch, user, ch.id, namespace, transactionManager ) );
    }


    /**
     * Closes a connection
     */
    @Override
    public void closeConnection( ConnectionHandle ch ) {
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "closeConnection( ConnectionHandle {} )", ch );
            }

            final PolyConnectionHandle connectionToClose = openConnections.remove( ch.id );
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
                    throw new GenericRuntimeException( e );
                }
            }

            for ( final String key : openStatements.keySet() ) {
                if ( key.startsWith( ch.id ) ) {
                    PolyStatementHandle<?> statementHandle = openStatements.remove( key );
                    statementHandle.unset();
                }
            }

            // TODO: release all resources associated with this connection
        }
    }


    private PolyConnectionHandle getPolyphenyDbConnectionHandle( String connectionId ) {
        if ( openConnections.containsKey( connectionId ) ) {
            return openConnections.get( connectionId );
        } else {
            throw new IllegalStateException( "Unknown connection id `" + connectionId + "`!" );
        }
    }


    private PolyStatementHandle<Object> getPolyphenyDbStatementHandle( StatementHandle h ) throws NoSuchStatementException {
        final PolyStatementHandle<Object> statement;
        if ( openStatements.containsKey( h.connectionId + "::" + h.id ) ) {
            statement = openStatements.get( h.connectionId + "::" + h.id );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "commit( ConnectionHandle {} )", ch );
            }
            Transaction transaction = connection.getCurrentTransaction();

            if ( transaction == null || !transaction.isActive() ) {
                if ( log.isTraceEnabled() ) {
                    log.trace( "No execute transaction for ConnectionHandle {}", connection );
                }
                return;
            }

            try {
                transaction.commit();
            } catch ( Throwable e ) {
                throw new GenericRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
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
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "rollback( ConnectionHandle {} )", ch );
            }
            Transaction transaction = connection.getCurrentTransaction();

            if ( transaction == null || !transaction.isActive() ) {
                if ( log.isTraceEnabled() ) {
                    log.trace( "No execute transaction for ConnectionHandle {}", connection );
                }
                return;
            }

            try {
                transaction.rollback();
            } catch ( TransactionException e ) {
                throw new GenericRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
            } finally {
                connection.endCurrentTransaction();
            }
        }
    }


    /**
     * Synchronizes client and server view of connection properties.
     * <p>
     * Note: this interface is considered "experimental" and may undergo further changes as this functionality is extended to other aspects of state management for
     * {@link Connection}, {@link Statement}, and {@link ResultSet}.
     */
    @Override
    public ConnectionProperties connectionSync( ConnectionHandle ch, ConnectionProperties connProps ) {
        final PolyConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
        synchronized ( connection ) {
            if ( log.isTraceEnabled() ) {
                log.trace( "connectionSync( ConnectionHandle {}, ConnectionProperties {} )", ch, connProps );
            }

            final PolyConnectionHandle connectionToSync = openConnections.get( ch.id );
            if ( connectionToSync == null ) {
                if ( log.isDebugEnabled() ) {
                    log.debug( "Connection {} is not execute.", ch.id );
                }
                throw new IllegalStateException( "Attempt to synchronize the connection `" + ch.id + "` with is either has not been execute yet or is already closed." );
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
            throw new GenericRuntimeException( e );
        }
    }


    public List<AvaticaParameter> deriveAvaticaParameters( AlgDataType parameterRowType ) {
        final List<AvaticaParameter> parameters = new ArrayList<>();
        for ( AlgDataTypeField field : parameterRowType.getFields() ) {
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

            for ( Entry<String, PolyConnectionHandle> entry : openConnections.entrySet() ) {
                String connectionId = entry.getKey();
                PolyConnectionHandle connectionHandle = entry.getValue();
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