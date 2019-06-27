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

package ch.unibas.dmi.dbis.polyphenydb.jdbc;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.PUID;
import ch.unibas.dmi.dbis.polyphenydb.PUID.ConnectionId;
import ch.unibas.dmi.dbis.polyphenydb.PUID.NodeId;
import ch.unibas.dmi.dbis.polyphenydb.PUID.Type;
import ch.unibas.dmi.dbis.polyphenydb.PUID.UserId;
import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.adapter.csv.CsvSchema;
import ch.unibas.dmi.dbis.polyphenydb.adapter.csv.CsvTable.Flavor;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.TableType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.TableType.PrimitiveTableType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.CatalogManagerImpl;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn.PrimitiveCatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase.PrimitiveCatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogEntity;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema.PrimitiveCatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable.PrimitiveCatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableTypeException;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.PolyphenyDbSignature;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplain;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelect;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParseException;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser.Config;
import ch.unibas.dmi.dbis.polyphenydb.tools.FrameworkConfig;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.tools.Planner;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.apache.calcite.avatica.remote.ProtobufMeta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.Unsafe;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DbmsMeta implements ProtobufMeta {

    private static DbmsMeta INSTANCE;

    private static final Logger LOG = LoggerFactory.getLogger( DbmsMeta.class );

    private static final ConcurrentMap<String, PolyphenyDbConnectionHandle> OPEN_CONNECTIONS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, PolyphenyDbStatementHandle> OPEN_STATEMENTS = new ConcurrentHashMap<>();


    static {
        INSTANCE = new DbmsMeta();
    }


    /**
     * Special value for {@code Statement#getLargeMaxRows()} that means fetch an unlimited number of rows in a single batch.
     *
     * Any other negative value will return an unlimited number of rows but will do it in the default batch size, namely 100.
     */
    public static final int UNLIMITED_COUNT = -2;

    final Calendar calendar = Unsafe.localCalendar();

    /**
     * Generates ids for statements. The ids are unique across all connections created by this JdbcMeta.
     */
    private final AtomicInteger statementIdGenerator = new AtomicInteger();


    public static DbmsMeta getInstance() {
        return INSTANCE;
    }


    /**
     * Creates a DbmsMeta
     */
    private DbmsMeta() {

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


    private MetaResultSet createMetaResultSet( final ConnectionHandle ch, final StatementHandle statementHandle, Map<String, Object> internalParameters, List<ColumnMetaData> columns, CursorFactory cursorFactory, final Frame firstFrame ) {
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
                        StatementType.SELECT ) {
                    @Override
                    public Enumerable<Object> enumerable( DataContext dataContext ) {
                        return Linq4j.asEnumerable( firstFrame.rows );
                    }
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


    private Enumerable<Object> toEnumerable( final List<? extends CatalogEntity> entities ) {
        final List<Object> objects = new LinkedList<>();
        for ( CatalogEntity entity : entities ) {
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
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getDatabaseProperties( ConnectionHandle {} )", ch );
        }

        final Map<DatabaseProperty, Object> map = new HashMap<>();
        // TODO

        LOG.error( "[NOT IMPLEMENTED YET] getDatabaseProperties( ConnectionHandle {} )", ch );
        return map;
    }


    public MetaResultSet getTables( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat tableNamePattern, final List<String> typeList ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getTables( ConnectionHandle {}, String {}, Pat {}, Pat {}, List<String> {} )", ch, catalog, schemaPattern, tableNamePattern, typeList );
        }
        try {
            final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
            final PolyXid xid = getCurrentTransaction( connection );
            List<TableType> types = null;
            if ( typeList != null ) {
                types = Catalog.convertTableTypeList( typeList );
            }
            final List<CatalogTable> tables = CatalogManagerImpl.getInstance().getTables( xid, catalog, schemaPattern, tableNamePattern, types );
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( tables ),
                    PrimitiveCatalogTable.class,
                    "NAME",
                    "SCHEMA",
                    "DATABASE",
                    "OWNER",
                    "ENCODING",
                    "COLLATION",
                    "TABLE_TYPE",
                    "DEFINITION"
            );
        } catch ( GenericCatalogException | UnknownTableTypeException e ) {
            throw propagate( e );
        }
    }



    @Override
    public MetaResultSet getColumns( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat tableNamePattern, final Pat columnNamePattern ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern, columnNamePattern );
        }
        try {
            final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
            final PolyXid xid = getCurrentTransaction( connection );
            final List<CatalogColumn> columns = CatalogManagerImpl.getInstance().getColumns( xid, catalog, schemaPattern, tableNamePattern, columnNamePattern );
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( columns ),
                    PrimitiveCatalogColumn.class,
                    "NAME",
                    "TABLE",
                    "SCHEMA",
                    "DATABASE",
                    "POSITION",
                    "TYPE",
                    "LENGTH",
                    "PRECISION",
                    "NULLABLE",
                    "ENCODING",
                    "COLLATION",
                    "AUTOINCREMENT_START_VALUE",
                    "AUTOINCREMENT_NEXT_VALUE",
                    "DEFAULT_VALUE",
                    "FORCE_DEFAULT"
            );
        } catch ( GenericCatalogException e ) {
            throw propagate( e );
        }
    }


    @Override
    public MetaResultSet getSchemas( final ConnectionHandle ch, final String catalog, final Pat schemaPattern ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getSchemas( ConnectionHandle {}, String {}, Pat {} )", ch, catalog, schemaPattern );
        }
        try {
            final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
            final PolyXid xid = getCurrentTransaction( connection );
            final List<CatalogSchema> schemas = CatalogManagerImpl.getInstance().getSchemas( xid, catalog, schemaPattern );
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( schemas ),
                    PrimitiveCatalogSchema.class,
                    "NAME",
                    "DATABASE",
                    "OWNER",
                    "ENCODING",
                    "COLLATION",
                    "SCHEMA_TYPE"
            );
        } catch ( GenericCatalogException e ) {
            throw propagate( e );
        }
    }


    @Override
    public MetaResultSet getCatalogs( final ConnectionHandle ch ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getCatalogs( ConnectionHandle {} )", ch );
        }
        try {
            final PolyphenyDbConnectionHandle connection = getPolyphenyDbConnectionHandle( ch.id );
            final PolyXid xid = getCurrentTransaction( connection );
            final List<CatalogDatabase> databases = CatalogManagerImpl.getInstance().getDatabases( xid );
            StatementHandle statementHandle = createStatement( ch );
            return createMetaResultSet(
                    ch,
                    statementHandle,
                    toEnumerable( databases ),
                    PrimitiveCatalogDatabase.class,
                    "NAME",
                    "OWNER",
                    "ENCODING",
                    "COLLATION",
                    "CONNECTION_LIMIT"
            );
        } catch ( GenericCatalogException e ) {
            throw propagate( e );
        }
    }


    @Override
    public MetaResultSet getTableTypes( final ConnectionHandle ch ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getTableTypes( ConnectionHandle {} )", ch );
        }
        final TableType[] tableTypes = TableType.values();
        final List<Object> objects = new LinkedList<>();
        for ( TableType tt : tableTypes ) {
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


    @Override
    public MetaResultSet getProcedures( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat procedureNamePattern ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getProcedures( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern );
        }

        LOG.error( "[NOT IMPLEMENTED YET] getProcedures( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getProcedureColumns( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat procedureNamePattern, final Pat columnNamePattern ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getProcedureColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern, columnNamePattern );
        }

        LOG.error( "[NOT IMPLEMENTED YET] getProcedureColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern, columnNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getColumnPrivileges( final ConnectionHandle ch, final String catalog, final String schema, final String table, final Pat columnNamePattern ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getColumnPrivileges( ConnectionHandle {}, String {}, String {}, String {}, Pat {} )", ch, catalog, schema, table, columnNamePattern );
        }

        // TODO

        LOG.error( "[NOT IMPLEMENTED YET] getColumnPrivileges( ConnectionHandle {}, String {}, String {}, String {}, Pat {} )", ch, catalog, schema, table, columnNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getTablePrivileges( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat tableNamePattern ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getTablePrivileges( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
        }

        // TODO

        LOG.error( "[NOT IMPLEMENTED YET] getTablePrivileges( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getBestRowIdentifier( final ConnectionHandle ch, final String catalog, final String schema, final String table, final int scope, final boolean nullable ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getBestRowIdentifier( ConnectionHandle {}, String {}, String {}, String {}, int {}, boolean {} )", ch, catalog, schema, table, scope, nullable );
        }

        LOG.error( "[NOT IMPLEMENTED YET] getBestRowIdentifier( ConnectionHandle {}, String {}, String {}, String {}, int {}, boolean {} )", ch, catalog, schema, table, scope, nullable );
        return null;
    }


    @Override
    public MetaResultSet getVersionColumns( final ConnectionHandle ch, final String catalog, final String schema, final String table ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getVersionColumns( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        }

        LOG.error( "[NOT IMPLEMENTED YET] getVersionColumns( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        return null;
    }


    @Override
    public MetaResultSet getPrimaryKeys( final ConnectionHandle ch, final String catalog, final String schema, final String table ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getPrimaryKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        }

        // TODO

        LOG.error( "[NOT IMPLEMENTED YET] getPrimaryKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        return null;
    }


    @Override
    public MetaResultSet getImportedKeys( final ConnectionHandle ch, final String catalog, final String schema, final String table ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getImportedKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        }

        // TODO

        LOG.error( "[NOT IMPLEMENTED YET] getImportedKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        return null;
    }


    @Override
    public MetaResultSet getExportedKeys( final ConnectionHandle ch, final String catalog, final String schema, final String table ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getExportedKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        }

        // TODO

        LOG.error( "[NOT IMPLEMENTED YET] getExportedKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        return null;
    }


    @Override
    public MetaResultSet getCrossReference( final ConnectionHandle ch, final String parentCatalog, final String parentSchema, final String parentTable, final String foreignCatalog, final String foreignSchema, final String foreignTable ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getCrossReference( ConnectionHandle {}, String {}, String {}, String {}, String {}, String {}, String {} )", ch, parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable );
        }

        // TODO

        LOG.error( "[NOT IMPLEMENTED YET] getCrossReference( ConnectionHandle {}, String {}, String {}, String {}, String {}, String {}, String {} )", ch, parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable );
        return null;
    }


    @Override
    public MetaResultSet getTypeInfo( final ConnectionHandle ch ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getTypeInfo( ConnectionHandle {} )", ch );
        }

        // TODO

        LOG.error( "[NOT IMPLEMENTED YET] getTypeInfo( ConnectionHandle {} )", ch );
        return null;
    }


    @Override
    public MetaResultSet getIndexInfo( final ConnectionHandle ch, final String catalog, final String schema, final String table, final boolean unique, final boolean approximate ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getIndexInfo( ConnectionHandle {}, String {}, String {}, String {}, boolean {}, boolean {} )", ch, catalog, schema, table, unique, approximate );
        }

        LOG.error( "[NOT IMPLEMENTED YET] getIndexInfo( ConnectionHandle {}, String {}, String {}, String {}, boolean {}, boolean {} )", ch, catalog, schema, table, unique, approximate );
        return null;
    }


    @Override
    public MetaResultSet getUDTs( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat typeNamePattern, final int[] types ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getUDTs( ConnectionHandle {}, String {}, Pat {}, Pat {}, int[] {} )", ch, catalog, schemaPattern, typeNamePattern, types );
        }

        LOG.error( "[NOT IMPLEMENTED YET] getUDTs( ConnectionHandle {}, String {}, Pat {}, Pat {}, int[] {} )", ch, catalog, schemaPattern, typeNamePattern, types );
        return null;
    }


    @Override
    public MetaResultSet getSuperTypes( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat typeNamePattern ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getSuperTypes( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern );
        }

        LOG.error( "[NOT IMPLEMENTED YET] getSuperTypes( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getSuperTables( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat tableNamePattern ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getSuperTables( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
        }

        LOG.error( "[NOT IMPLEMENTED YET] getSuperTables( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getAttributes( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat typeNamePattern, final Pat attributeNamePattern ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getAttributes( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern, attributeNamePattern );
        }

        LOG.error( "[NOT IMPLEMENTED YET] getAttributes( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern, attributeNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getClientInfoProperties( final ConnectionHandle ch ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getClientInfoProperties( ConnectionHandle {} )", ch );
        }

        LOG.error( "[NOT IMPLEMENTED YET] getClientInfoProperties( ConnectionHandle {} )", ch );
        return null;
    }


    @Override
    public MetaResultSet getFunctions( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat functionNamePattern ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getFunctions( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern );
        }

        LOG.error( "[NOT IMPLEMENTED YET] getFunctions( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getFunctionColumns( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat functionNamePattern, final Pat columnNamePattern ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getFunctionColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern, columnNamePattern );
        }

        LOG.error( "[NOT IMPLEMENTED YET] getFunctionColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern, columnNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getPseudoColumns( final ConnectionHandle ch, final String catalog, final Pat schemaPattern, final Pat tableNamePattern, final Pat columnNamePattern ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "getPseudoColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern, columnNamePattern );
        }

        LOG.error( "[NOT IMPLEMENTED YET] getPseudoColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern, columnNamePattern );
        return null;
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
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "executeBatchProtobuf( StatementHandle {}, List<UpdateBatch> {} )", h, parameterValues );
        }

        LOG.error( "[NOT IMPLEMENTED YET] executeBatchProtobuf( StatementHandle {}, List<UpdateBatch> {} )", h, parameterValues );
        return null;
    }


    /**
     * Creates an iterable for a result set.
     *
     * The default implementation just returns {@code iterable}, which it requires to be not null; derived classes may instead choose to execute the relational
     * expression in {@code signature}.
     */
    @Override
    public Iterable<Object> createIterable( final StatementHandle stmt, final QueryState state, final Signature signature, final List<TypedValue> parameters, final Frame firstFrame ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "createIterable( StatementHandle {}, QueryState {}, Signature {}, List<TypedValue> {}, Frame {} )", stmt, state, signature, parameters, firstFrame );
        }

        LOG.error( "[NOT IMPLEMENTED YET] createIterable( StatementHandle {}, QueryState {}, Signature {}, List<TypedValue> {}, Frame {} )", stmt, state, signature, parameters, firstFrame );
        return null;
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
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "prepare( ConnectionHandle {}, String {}, long {} )", ch, sql, maxRowCount );
        }

        LOG.error( "[NOT IMPLEMENTED YET] prepare( ConnectionHandle {}, String {}, long {} )", ch, sql, maxRowCount );
        return null;
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
    public ExecuteResult prepareAndExecute( final StatementHandle h, final String sql, final long maxRowCount, final PrepareCallback callback ) throws NoSuchStatementException {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "prepareAndExecute( StatementHandle {}, String {}, long {}, PrepareCallback {} )", h, sql, maxRowCount, callback );
        }
        return prepareAndExecute( h, sql, maxRowCount, AvaticaUtils.toSaturatedInt( maxRowCount ), callback );
    }


    /**
     * Prepares and executes a statement.
     *
     * @param h Statement handle
     * @param sql SQL query
     * @param maxRowCount Maximum number of rows for the entire query. Negative for no limit (different meaning than JDBC).
     * @param maxRowsInFirstFrame Maximum number of rows for the first frame. This value should always be less than or equal to {@code maxRowCount} as the number of results are guaranteed     * to be restricted by {@code maxRowCount} and the underlying database.
     * @param callback Callback to lock, clear and assign cursor
     * @return Result containing statement ID, and if a query, a result set and first frame of data
     */
    @Override
    public ExecuteResult prepareAndExecute( final StatementHandle h, final String sql, final long maxRowCount, final int maxRowsInFirstFrame, final PrepareCallback callback ) throws NoSuchStatementException {

        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "prepareAndExecute( StatementHandle {}, String {}, long {}, int {}, PrepareCallback {} )", h, sql, maxRowCount, maxRowsInFirstFrame, callback );
        }
        final StopWatch stopWatch = new StopWatch();

        final PolyphenyDbConnectionHandle connection = OPEN_CONNECTIONS.get( h.connectionId );
        final PolyphenyDbStatementHandle statement;
        synchronized ( OPEN_STATEMENTS ) {
            if ( OPEN_STATEMENTS.containsKey( h.connectionId + "::" + Integer.toString( h.id ) ) ) {
                statement = OPEN_STATEMENTS.get( h.connectionId + "::" + Integer.toString( h.id ) );
            } else {
                throw new RuntimeException( "Ok.... In this case there was no statement created before. Find out why." );
            }
        }

        // //////////////////////////
        // For testing
        // /////////////////////////
        final SchemaPlus rootSchema = Frameworks.createRootSchema( false );

        // CSV
        File csvDir = new File( "testTestCsv" );
        rootSchema.add( "CSV", new CsvSchema( csvDir, Flavor.FILTERABLE ) );

        ///////////////////
        // (1)  Configure //
        ///////////////////
        SqlParser.ConfigBuilder configConfigBuilder = SqlParser.configBuilder();
        configConfigBuilder.setCaseSensitive( false );
        Config parserConfig = configConfigBuilder.build();

        // SqlToRelConverter.ConfigBuilder sqlToRelConfigBuilder = SqlToRelConverter.configBuilder();
        // SqlToRelConverter.Config sqlToRelConfig = sqlToRelConfigBuilder.build();

        // List<RelTraitDef> traitDefs = new ArrayList<>();
        //traitDefs.add( ConventionTraitDef.INSTANCE );
        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig( parserConfig )
                //            .traitDefs( traitDefs )
                .defaultSchema( rootSchema )
                //             .sqlToRelConverterConfig( sqlToRelConfig )
                //             .programs( Programs.ofRules( Programs.RULE_SET ) )
                .build();
        //.programs( Programs.ofRules( Programs.CALC_RULES ) );

        ///////////////////
        // (2)  PARSING  //
        ///////////////////
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Parsing PolySQL statement ..." );
        }
        stopWatch.start();

        Planner planner = Frameworks.getPlanner( frameworkConfig );
        SqlNode parsed;
        try {
            parsed = planner.parse( sql );
        } catch ( SqlParseException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }
        stopWatch.stop();
        if ( LOG.isTraceEnabled() ) {
            LOG.debug( "Parsed query: [{}]", parsed );
        }
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Parsing PolySQL statement ... done. [{}]", stopWatch );
        }

        /////////
        // (2.5) TRANSACTION ID
        ////////
        PolyXid xid = getCurrentTransaction( connection );


        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Building response ... done. [{}]", stopWatch );
        }

        ExecuteResult result = null;
        switch ( parsed.getKind() ) {
            case SELECT:
                result = DmlExecutionEngine.getInstance().executeSelect( h, statement, maxRowsInFirstFrame, maxRowCount, planner, stopWatch, (SqlSelect) parsed );
                break;

            case INSERT:
            case DELETE:
            case UPDATE:
            case MERGE:
            case PROCEDURE_CALL:
                result = DmlExecutionEngine.getInstance().executeDml( h, statement, maxRowsInFirstFrame, maxRowCount, planner, stopWatch, parsed );
                break;

            case COMMIT:
            case ROLLBACK:
                System.out.println( ":) TRX Control" );
                break;

            case CREATE_SCHEMA:
            case DROP_SCHEMA:
            case CREATE_TABLE:
            case ALTER_TABLE:
            case DROP_TABLE:
            case CREATE_VIEW:
            case ALTER_VIEW:
            case DROP_VIEW:
            case CREATE_MATERIALIZED_VIEW:
            case ALTER_MATERIALIZED_VIEW:
            case DROP_MATERIALIZED_VIEW:
            case CREATE_INDEX:
            case ALTER_INDEX:
            case DROP_INDEX:
                result = DdlExecutionEngine.getInstance().execute( h, statement, planner, stopWatch, rootSchema, parserConfig, parsed );
                break;

            case EXPLAIN:
                if ( ((SqlExplain) parsed).getExplicandum().getKind() == SqlKind.SELECT ) {
                    result = DmlExecutionEngine.getInstance().explain( h, statement, planner, stopWatch, (SqlExplain) parsed );
                } else {
                    throw new RuntimeException( "EXPLAIN is currently only supported for SELECT queries!" );
                }
                break;

            default:
                throw new RuntimeException( "Unknown or unsupported query type: " + parsed.getKind().name() );
        }

        return result;
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
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "prepareAndExecuteBatch( StatementHandle {}, List<String> {} )", h, sqlCommands );
        }

        LOG.error( "[NOT IMPLEMENTED YET] prepareAndExecuteBatch( StatementHandle {}, List<String> {} )", h, sqlCommands );
        return null;
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
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "executeBatch( StatementHandle {}, List<List<TypedValue>> {} )", h, parameterValues );
        }

        LOG.error( "[NOT IMPLEMENTED YET] executeBatch( StatementHandle {}, List<List<TypedValue>> {} )", h, parameterValues );
        return null;
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
    public Frame fetch( final StatementHandle h, final long offset, final int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "fetch( StatementHandle {}, long {}, int {} )", h, offset, fetchMaxRowCount );
        }

        final PolyphenyDbConnectionHandle connection = OPEN_CONNECTIONS.get( h.connectionId );
        final PolyphenyDbStatementHandle statement;
        synchronized ( OPEN_STATEMENTS ) {
            if ( OPEN_STATEMENTS.containsKey( h.connectionId + "::" + Integer.toString( h.id ) ) ) {
                statement = OPEN_STATEMENTS.get( h.connectionId + "::" + Integer.toString( h.id ) );
            } else {
                throw new NoSuchStatementException( h );
            }
        }

        PolyphenyDbResultSet openResultSet = statement.getOpenResultSet();
        if ( openResultSet == null ) {
            throw new MissingResultsException( h );
        }

        final List<Object> rows = new LinkedList<>();
        boolean done = false;
        try {
            int numberOfColumns = openResultSet.getMetaData().getColumnCount();
            for ( int rowCount = 0; openResultSet.next() && (fetchMaxRowCount < 0 || rowCount < fetchMaxRowCount); ++rowCount ) {
                Object[] array = new Object[numberOfColumns];
                for ( int i = 0; i < numberOfColumns; i++ ) {
                    array[i] = openResultSet.getObject( i + 1 );
                }
                rows.add( array );
            }
            done = openResultSet.isAfterLast();
            return new Frame( offset, done, rows );
        } catch ( SQLException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), e.getErrorCode(), e.getSQLState(), AvaticaSeverity.ERROR );
        } finally {
            if ( done ) {
                openResultSet.close();
                statement.setOpenResultSet( null );
                if ( connection.isAutoCommit() ) {
//                commit( connection.getHandle() );
                }
            }
        }
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
    public ExecuteResult execute( final StatementHandle h, final List<TypedValue> parameterValues, final long maxRowCount ) throws NoSuchStatementException {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "execute( StatementHandle {}, List<TypedValue> {}, long {} )", h, parameterValues, maxRowCount );
        }

        LOG.error( "[NOT IMPLEMENTED YET] execute( StatementHandle {}, List<TypedValue> {}, long {} )", h, parameterValues, maxRowCount );
        return null;
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
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "execute( StatementHandle {}, List<TypedValue> {}, int {} )", h, parameterValues, maxRowsInFirstFrame );
        }

        LOG.error( "[NOT IMPLEMENTED YET] execute( StatementHandle {}, List<TypedValue> {}, int {} )", h, parameterValues, maxRowsInFirstFrame );
        return null;
    }


    /**
     * Called during the creation of a statement to allocate a new handle.
     *
     * @param ch Connection handle
     */
    @Override
    public StatementHandle createStatement( ConnectionHandle ch ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "createStatement( ConnectionHandle {} )", ch );
        }

        final PolyphenyDbConnectionHandle connection = OPEN_CONNECTIONS.get( ch.id );
        final PolyphenyDbStatementHandle statement;
        synchronized ( OPEN_STATEMENTS ) {
            final int id = statementIdGenerator.getAndIncrement();
            statement = new PolyphenyDbStatementHandle( connection, id );
            OPEN_STATEMENTS.put( ch.id + "::" + id, statement );
        }
        StatementHandle h = new StatementHandle( ch.id, statement.getStatementId(), null );
        LOG.trace( "created statement {}", h );
        return h;
    }


    /*
    private void registerMetaStatement( final StatementHandle statementHandle, final PolyphenyDbResultSet rs ) throws SQLException {
        final PolyphenyDbStatementHandle statement;
        synchronized ( OPEN_STATEMENTS ) {
            if ( OPEN_STATEMENTS.containsKey( statementHandle.connectionId + "::" + Integer.toString( statementHandle.id ) ) ) {
                statement = OPEN_STATEMENTS.get( statementHandle.connectionId + "::" + Integer.toString( statementHandle.id ) );
            } else {
                throw new RuntimeException( "There is no corresponding statement" );
            }
        }
        statement.setOpenResultSet( rs );
    }
*/

    /**
     * Closes a statement.
     *
     * If the statement handle is not known, or is already closed, does nothing.
     *
     * @param statementHandle Statement handle
     */
    @Override
    public void closeStatement( final StatementHandle statementHandle ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "closeStatement( StatementHandle {} )", statementHandle );
        }

        final PolyphenyDbStatementHandle toClose = OPEN_STATEMENTS.remove( statementHandle.connectionId + "::" + Integer.toString( statementHandle.id ) );
        if ( toClose != null ) {
            toClose.setOpenResultSet( null ); // closes the currently open ResultSet
        }
    }


    /**
     * Opens (creates) a connection. The client allocates its own connection ID which the server is then made aware of through the {@link ConnectionHandle}.
     * The Map {@code info} argument is analogous to the {@link Properties} typically passed to a "normal" JDBC Driver. Avatica specific properties should not
     * be included -- only properties for the underlying driver.
     *
     * @param ch A ConnectionHandle encapsulates information about the connection to be opened as provided by the client.
     * @param connectionParameters A Map corresponding to the Properties typically passed to a JDBC Driver.
     */
    @Override
    public void openConnection( final ConnectionHandle ch, final Map<String, String> connectionParameters ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "openConnection( ConnectionHandle {}, Map<String, String> {} )", ch, connectionParameters );
        }

        final PolyphenyDbConnectionHandle connectionToOpen;
        synchronized ( OPEN_CONNECTIONS ) {
            if ( OPEN_CONNECTIONS.containsKey( ch.id ) ) {
                if ( LOG.isDebugEnabled() ) {
                    LOG.debug( "Key {} is already present in the OPEN_CONNECTIONS map.", ch.id );
                }
                throw new IllegalStateException( "Forbidden attempt to open the connection `" + ch.id + "` twice!" );
            }

            if ( LOG.isDebugEnabled() ) {
                LOG.debug( "Creating a new connection." );
            }

            final CatalogUser user;
            try {
                user = Authenticator.authenticate(
                        connectionParameters.getOrDefault( "username", connectionParameters.get( "user" ) ),
                        connectionParameters.getOrDefault( "password", "" ) );
            } catch ( AuthenticationException e ) {
                throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
            }
            assert user != null;

            String databaseName = connectionParameters.getOrDefault( "database", connectionParameters.get( "db" ) );
            if ( databaseName == null || databaseName.isEmpty() ) {
                databaseName = "APP";
            }
            String schemaName = connectionParameters.get( "schema" );
            if ( schemaName == null || schemaName.isEmpty() ) {
                schemaName = "public";
            }

            final NodeId nodeId = (NodeId) PUID.randomPUID( Type.NODE ); // TODO: get real node id -- configuration.get("nodeid")
            final UserId userId = (UserId) PUID.randomPUID( Type.USER ); // TODO: get real user id -- connectionParameters.get("user")

            // Create transaction id
            PolyXid xid = PolyphenyDbConnectionHandle.generateNewTransactionId( nodeId, userId, ConnectionId.fromString( ch.id ) );

            final Catalog catalog = CatalogManagerImpl.getInstance().getCatalog();
            // Check database access
            final CatalogDatabase database;
            try {
                database = catalog.getDatabase( xid, databaseName );
            } catch ( GenericCatalogException | UnknownDatabaseException e ) {
                throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
            }
            assert database != null;

//            Authorizer.hasAccess( user, database );

            // Check schema access
            final CatalogSchema schema;
            try {
                schema = catalog.getSchema( xid, database.name, schemaName );
            } catch ( GenericCatalogException | UnknownSchemaException e ) {
                throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
            }
            assert schema != null;

//            Authorizer.hasAccess( user, schema );

            connectionToOpen = new PolyphenyDbConnectionHandle( ch, nodeId, user, ch.id, database, schema, xid );

            OPEN_CONNECTIONS.put( ch.id, connectionToOpen );
        }
    }


    /**
     * Closes a connection
     */
    @Override
    public void closeConnection( ConnectionHandle ch ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "closeConnection( ConnectionHandle {} )", ch );
        }

        final PolyphenyDbConnectionHandle connectionToClose = OPEN_CONNECTIONS.remove( ch.id );
        if ( connectionToClose == null ) {
            if ( LOG.isDebugEnabled() ) {
                LOG.debug( "Connection {} already closed.", ch.id );
            }
            return;
        }

        synchronized ( OPEN_STATEMENTS ) {
            for ( final String key : OPEN_STATEMENTS.keySet() ) {
                if ( key.startsWith( ch.id ) ) {
                    OPEN_STATEMENTS.remove( key ).setOpenResultSet( null );
                }
            }
        }

        // TODO: release all resources associated with this connection
        LOG.error( "[NOT IMPLEMENTED YET] closeConnection( ConnectionHandle {} )", ch );
    }


    private PolyphenyDbConnectionHandle getPolyphenyDbConnectionHandle( String connectionId ) {
        if ( OPEN_CONNECTIONS.containsKey( connectionId ) ) {
            return OPEN_CONNECTIONS.get( connectionId );
        } else {
            throw new IllegalStateException( "Unknown connection id `" + connectionId + "`!" );
        }
    }


    private PolyXid getCurrentTransaction( PolyphenyDbConnectionHandle connection ) {
        final PolyXid currentTransaction;
        final boolean beginOfTransaction;
        synchronized ( this ) {
            if ( connection.getCurrentTransaction() == null ) {
                currentTransaction = connection.startNewTransaction();
                beginOfTransaction = true;
                if ( LOG.isTraceEnabled() ) {
                    LOG.trace( "Required a new TransactionId: {}", currentTransaction );
                }
            } else {
                currentTransaction = connection.getCurrentTransaction();
                beginOfTransaction = false;
                if ( LOG.isTraceEnabled() ) {
                    LOG.trace( "Reusing the current TransactionId: {}", currentTransaction );
                }
            }
        }
        return currentTransaction;
    }


    /**
     * Re-sets the {@link ResultSet} on a Statement. Not a JDBC method.
     *
     * @return True if there are results to fetch after resetting to the given offset. False otherwise
     */
    @Override
    public boolean syncResults( final StatementHandle sh, final QueryState state, final long offset ) throws NoSuchStatementException {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "syncResults( StatementHandle {}, QueryState {}, long {} )", sh, state, offset );
        }

        LOG.error( "[NOT IMPLEMENTED YET] syncResults( StatementHandle {}, QueryState {}, long {} )", sh, state, offset );
        return false;
    }


    /**
     * Makes all changes since the last commit/rollback permanent. Analogous to {@link Connection#commit()}.
     *
     * @param ch A reference to the real JDBC Connection
     */
    @Override
    public void commit( final ConnectionHandle ch ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "commit( ConnectionHandle {} )", ch );
        }
        throw new UnsupportedOperationException();
    }


    /**
     * Undoes all changes since the last commit/rollback. Analogous to {@link Connection#rollback()};
     *
     * @param ch A reference to the real JDBC Connection
     */
    @Override
    public void rollback( final ConnectionHandle ch ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "rollback( ConnectionHandle {} )", ch );
        }
        throw new UnsupportedOperationException();
    }



    /**
     * Synchronizes client and server view of connection properties.
     *
     * Note: this interface is considered "experimental" and may undergo further changes as this functionality is extended to other aspects of state management for
     * {@link Connection}, {@link Statement}, and {@link ResultSet}.
     */
    @Override
    public ConnectionProperties connectionSync( ConnectionHandle ch, ConnectionProperties connProps ) {
        if ( LOG.isTraceEnabled() ) {
            LOG.trace( "connectionSync( ConnectionHandle {}, ConnectionProperties {} )", ch, connProps );
        }

        final PolyphenyDbConnectionHandle connectionToSync = OPEN_CONNECTIONS.get( ch.id );
        if ( connectionToSync == null ) {
            if ( LOG.isDebugEnabled() ) {
                LOG.debug( "Connection {} is not open.", ch.id );
            }
            throw new IllegalStateException( "Attempt to synchronize the connection `" + ch.id + "` with is either has not been open yet or is already closed." );
        }

        return connectionToSync.mergeConnectionProperties( connProps );
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

}