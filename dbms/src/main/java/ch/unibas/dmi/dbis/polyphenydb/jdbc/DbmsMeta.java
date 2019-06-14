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


import static org.apache.calcite.avatica.remote.MetricsHelper.concat;

import ch.unibas.dmi.dbis.polyphenydb.PUID;
import ch.unibas.dmi.dbis.polyphenydb.PUID.Type;
import ch.unibas.dmi.dbis.polyphenydb.PolyXid;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JdbcMeta.ConnectionCacheSettings;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.JdbcMeta.StatementCacheSettings;
import ch.unibas.dmi.dbis.polyphenydb.scu.catalog.CatalogManagerImpl;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchConnectionException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.noop.NoopMetricsSystem;
import org.apache.calcite.avatica.proto.Requests.UpdateBatch;
import org.apache.calcite.avatica.remote.ProtobufMeta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.Unsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DbmsMeta implements ProtobufMeta {

    private static DbmsMeta INSTANCE;

    private static final Logger LOG = LoggerFactory.getLogger( DbmsMeta.class );

    private static final String CONN_CACHE_KEY_BASE = "avatica.connectioncache";

    private static final String STMT_CACHE_KEY_BASE = "avatica.statementcache";


    static {
        try {
            INSTANCE = new DbmsMeta( "jdbc:polyphenydbembedded:" );
        } catch ( SQLException e ) {
            LOG.error( "Exception while creating DBMS instance.", e );
        }
    }


    public static DbmsMeta getInstance() {
        return INSTANCE;
    }


    /**
     * Special value for {@code Statement#getLargeMaxRows()} that means fetch an unlimited number of rows in a single batch.
     *
     * Any other negative value will return an unlimited number of rows but will do it in the default batch size, namely 100.
     */
    public static final int UNLIMITED_COUNT = -2;

    // End of constants, start of member variables

    final Calendar calendar = Unsafe.localCalendar();

    /**
     * Generates ids for statements. The ids are unique across all connections created by this JdbcMeta.
     */
    private final AtomicInteger statementIdGenerator = new AtomicInteger();


    private final String url;
    private final Properties info;
    private final Cache<String, Connection> connectionCache;
    private final Cache<Integer, StatementInfo> statementCache;
    private final MetricsSystem metrics;


    /**
     * Creates a JdbcMeta.
     *
     * @param url a database url of the form <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
     */
    public DbmsMeta( String url ) throws SQLException {
        this( url, new Properties() );
    }


    /**
     * Creates a JdbcMeta.
     *
     * @param url a database url of the form <code>jdbc:<em>subprotocol</em>:<em>subname</em></code>
     * @param user the database user on whose behalf the connection is being made
     * @param password the user's password
     */
    public DbmsMeta( final String url, final String user, final String password ) throws SQLException {
        this( url, new Properties() {
            {
                put( "user", user );
                put( "password", password );
            }
        } );
    }


    public DbmsMeta( String url, Properties info ) throws SQLException {
        this( url, info, NoopMetricsSystem.getInstance() );
    }


    /**
     * Creates a JdbcMeta.
     *
     * @param url a database url of the form <code> jdbc:<em>subprotocol</em>:<em>subname</em></code>
     * @param info a list of arbitrary string tag/value pairs as connection arguments; normally at least a "user" and "password" property should be included
     */
    public DbmsMeta( String url, Properties info, MetricsSystem metrics ) throws SQLException {
        this.url = url;
        this.info = info;
        this.metrics = Objects.requireNonNull( metrics );

        int concurrencyLevel = Integer.parseInt( info.getProperty( ConnectionCacheSettings.CONCURRENCY_LEVEL.key(), ConnectionCacheSettings.CONCURRENCY_LEVEL.defaultValue() ) );
        int initialCapacity = Integer.parseInt( info.getProperty( ConnectionCacheSettings.INITIAL_CAPACITY.key(), ConnectionCacheSettings.INITIAL_CAPACITY.defaultValue() ) );
        long maxCapacity = Long.parseLong( info.getProperty( ConnectionCacheSettings.MAX_CAPACITY.key(), ConnectionCacheSettings.MAX_CAPACITY.defaultValue() ) );
        long connectionExpiryDuration = Long.parseLong( info.getProperty( ConnectionCacheSettings.EXPIRY_DURATION.key(), ConnectionCacheSettings.EXPIRY_DURATION.defaultValue() ) );
        TimeUnit connectionExpiryUnit = TimeUnit.valueOf( info.getProperty( ConnectionCacheSettings.EXPIRY_UNIT.key(), ConnectionCacheSettings.EXPIRY_UNIT.defaultValue() ) );
        this.connectionCache = CacheBuilder.newBuilder()
                .concurrencyLevel( concurrencyLevel )
                .initialCapacity( initialCapacity )
                .maximumSize( maxCapacity )
                .expireAfterAccess( connectionExpiryDuration, connectionExpiryUnit )
                .removalListener( new ConnectionExpiryHandler() )
                .build();
        LOG.debug( "instantiated connection cache: {}", connectionCache.stats() );

        concurrencyLevel = Integer.parseInt( info.getProperty( StatementCacheSettings.CONCURRENCY_LEVEL.key(), StatementCacheSettings.CONCURRENCY_LEVEL.defaultValue() ) );
        initialCapacity = Integer.parseInt( info.getProperty( StatementCacheSettings.INITIAL_CAPACITY.key(), StatementCacheSettings.INITIAL_CAPACITY.defaultValue() ) );
        maxCapacity = Long.parseLong( info.getProperty( StatementCacheSettings.MAX_CAPACITY.key(), StatementCacheSettings.MAX_CAPACITY.defaultValue() ) );
        connectionExpiryDuration = Long.parseLong( info.getProperty( StatementCacheSettings.EXPIRY_DURATION.key(), StatementCacheSettings.EXPIRY_DURATION.defaultValue() ) );
        connectionExpiryUnit = TimeUnit.valueOf( info.getProperty( StatementCacheSettings.EXPIRY_UNIT.key(), StatementCacheSettings.EXPIRY_UNIT.defaultValue() ) );
        this.statementCache = CacheBuilder.newBuilder()
                .concurrencyLevel( concurrencyLevel )
                .initialCapacity( initialCapacity )
                .maximumSize( maxCapacity )
                .expireAfterAccess( connectionExpiryDuration, connectionExpiryUnit )
                .removalListener( new StatementExpiryHandler() )
                .build();

        LOG.debug( "instantiated statement cache: {}", statementCache.stats() );

        // Register some metrics
        this.metrics.register( concat( JdbcMeta.class, "ConnectionCacheSize" ), () -> connectionCache.size() );

        this.metrics.register( concat( JdbcMeta.class, "StatementCacheSize" ), () -> statementCache.size() );
    }


    // For testing purposes
    protected AtomicInteger getStatementIdGenerator() {
        return statementIdGenerator;
    }


    // For testing purposes
    protected Cache<String, Connection> getConnectionCache() {
        return connectionCache;
    }


    // For testing purposes
    protected Cache<Integer, StatementInfo> getStatementCache() {
        return statementCache;
    }


    /**
     * Converts from JDBC metadata to Avatica columns.
     */
    protected static List<ColumnMetaData>
    columns( ResultSetMetaData metaData ) throws SQLException {
        if ( metaData == null ) {
            return Collections.emptyList();
        }
        final List<ColumnMetaData> columns = new ArrayList<>();
        for ( int i = 1; i <= metaData.getColumnCount(); i++ ) {
            final SqlType sqlType = SqlType.valueOf( metaData.getColumnType( i ) );
            final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of( sqlType.internal );
            final ColumnMetaData.AvaticaType t;
            if ( sqlType == SqlType.ARRAY || sqlType == SqlType.STRUCT || sqlType == SqlType.MULTISET ) {
                ColumnMetaData.AvaticaType arrayValueType = ColumnMetaData.scalar( Types.JAVA_OBJECT, metaData.getColumnTypeName( i ), ColumnMetaData.Rep.OBJECT );
                t = ColumnMetaData.array( arrayValueType, metaData.getColumnTypeName( i ), rep );
            } else {
                t = ColumnMetaData.scalar( metaData.getColumnType( i ), metaData.getColumnTypeName( i ), rep );
            }
            ColumnMetaData md =
                    new ColumnMetaData( i - 1, metaData.isAutoIncrement( i ),
                            metaData.isCaseSensitive( i ), metaData.isSearchable( i ),
                            metaData.isCurrency( i ), metaData.isNullable( i ),
                            metaData.isSigned( i ), metaData.getColumnDisplaySize( i ),
                            metaData.getColumnLabel( i ), metaData.getColumnName( i ),
                            metaData.getSchemaName( i ), metaData.getPrecision( i ),
                            metaData.getScale( i ), metaData.getTableName( i ),
                            metaData.getCatalogName( i ), t, metaData.isReadOnly( i ),
                            metaData.isWritable( i ), metaData.isDefinitelyWritable( i ),
                            metaData.getColumnClassName( i ) );
            columns.add( md );
        }
        return columns;
    }


    /**
     * Converts from JDBC metadata to Avatica parameters
     */
    protected static List<AvaticaParameter> parameters( ParameterMetaData metaData ) throws SQLException {
        if ( metaData == null ) {
            return Collections.emptyList();
        }
        final List<AvaticaParameter> params = new ArrayList<>();
        for ( int i = 1; i <= metaData.getParameterCount(); i++ ) {
            params.add(
                    new AvaticaParameter(
                            metaData.isSigned( i ),
                            metaData.getPrecision( i ),
                            metaData.getScale( i ),
                            metaData.getParameterType( i ),
                            metaData.getParameterTypeName( i ),
                            metaData.getParameterClassName( i ),
                            "?" + i ) );
        }
        return params;
    }


    protected static Signature signature( ResultSetMetaData metaData, ParameterMetaData parameterMetaData, String sql, StatementType statementType ) throws SQLException {
        final CursorFactory cf = CursorFactory.LIST;  // because JdbcResultSet#frame
        return new Signature( columns( metaData ), sql, parameters( parameterMetaData ), null, cf, statementType );
    }


    protected static Signature signature( ResultSetMetaData metaData ) throws SQLException {
        return signature( metaData, null, null, null );
    }


    public Map<DatabaseProperty, Object> getDatabaseProperties( ConnectionHandle ch ) {
        try {
            final Map<DatabaseProperty, Object> map = new HashMap<>();
            final Connection conn = getConnection( ch.id );
            final DatabaseMetaData metaData = conn.getMetaData();
            for ( DatabaseProperty p : DatabaseProperty.values() ) {
                addProperty( map, metaData, p );
            }
            return map;
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    private static Object addProperty( Map<DatabaseProperty, Object> map, DatabaseMetaData metaData, DatabaseProperty p ) throws SQLException {
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


    protected Connection getConnection( String id ) throws SQLException {
        if ( id == null ) {
            throw new NullPointerException( "Connection id is null." );
        }
        Connection conn = connectionCache.getIfPresent( id );
        if ( conn == null ) {
            throw new NoSuchConnectionException( "Connection not found: invalid id, closed, or expired: " + id );
        }
        return conn;
    }


    public MetaResultSet getTables( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> typeList ) {
        final PolyXid randomXid = PolyXid.generateLocalTransactionIdentifier( PUID.randomPUID( Type.TRANSACTION ), PUID.randomPUID( Type.TRANSACTION ) );
        try {
            List<CatalogTable> tables = CatalogManagerImpl.getInstance().getTables( randomXid, catalog, schemaPattern, tableNamePattern );

            Connection connection = getConnection( ch.id );

            //
            //  TODO
            //

            final ResultSet rs = null;
            int stmtId = registerMetaStatement( rs );
            return JdbcResultSet.create( ch.id, stmtId, rs );
        } catch ( GenericCatalogException | SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * Registers a StatementInfo for the given ResultSet, returning the id under which it is registered. This should be used for metadata ResultSets, which have an implicit statement created.
     */
    private int registerMetaStatement( ResultSet rs ) throws SQLException {
        final int id = statementIdGenerator.getAndIncrement();
        StatementInfo statementInfo = new StatementInfo( rs.getStatement() );
        statementInfo.setResultSet( rs );
        statementCache.put( id, statementInfo );
        return id;
    }


    /**
     * Executes a batch of commands on a prepared statement.
     *
     * @param h Statement handle
     * @param parameterValues A collection of list of typed values, one list per batch
     * @return An array of update counts containing one element for each command in the batch.
     */
    @Override
    public ExecuteBatchResult executeBatchProtobuf( StatementHandle h, List<UpdateBatch> parameterValues ) throws NoSuchStatementException {
        return null;
    }


    @Override
    public MetaResultSet getColumns( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern ) {
        return null;
    }


    @Override
    public MetaResultSet getSchemas( ConnectionHandle ch, String catalog, Pat schemaPattern ) {
        return null;
    }


    @Override
    public MetaResultSet getCatalogs( ConnectionHandle ch ) {
        return null;
    }


    @Override
    public MetaResultSet getTableTypes( ConnectionHandle ch ) {
        return null;
    }


    @Override
    public MetaResultSet getProcedures( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat procedureNamePattern ) {
        return null;
    }


    @Override
    public MetaResultSet getProcedureColumns( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat procedureNamePattern, Pat columnNamePattern ) {
        return null;
    }


    @Override
    public MetaResultSet getColumnPrivileges( ConnectionHandle ch, String catalog, String schema, String table, Pat columnNamePattern ) {
        return null;
    }


    @Override
    public MetaResultSet getTablePrivileges( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern ) {
        return null;
    }


    @Override
    public MetaResultSet getBestRowIdentifier( ConnectionHandle ch, String catalog, String schema, String table, int scope, boolean nullable ) {
        return null;
    }


    @Override
    public MetaResultSet getVersionColumns( ConnectionHandle ch, String catalog, String schema, String table ) {
        return null;
    }


    @Override
    public MetaResultSet getPrimaryKeys( ConnectionHandle ch, String catalog, String schema, String table ) {
        return null;
    }


    @Override
    public MetaResultSet getImportedKeys( ConnectionHandle ch, String catalog, String schema, String table ) {
        return null;
    }


    @Override
    public MetaResultSet getExportedKeys( ConnectionHandle ch, String catalog, String schema, String table ) {
        return null;
    }


    @Override
    public MetaResultSet getCrossReference( ConnectionHandle ch, String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable ) {
        return null;
    }


    @Override
    public MetaResultSet getTypeInfo( ConnectionHandle ch ) {
        return null;
    }


    @Override
    public MetaResultSet getIndexInfo( ConnectionHandle ch, String catalog, String schema, String table, boolean unique, boolean approximate ) {
        return null;
    }


    @Override
    public MetaResultSet getUDTs( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern, int[] types ) {
        return null;
    }


    @Override
    public MetaResultSet getSuperTypes( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern ) {
        return null;
    }


    @Override
    public MetaResultSet getSuperTables( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern ) {
        return null;
    }


    @Override
    public MetaResultSet getAttributes( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern, Pat attributeNamePattern ) {
        return null;
    }


    @Override
    public MetaResultSet getClientInfoProperties( ConnectionHandle ch ) {
        return null;
    }


    @Override
    public MetaResultSet getFunctions( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat functionNamePattern ) {
        return null;
    }


    @Override
    public MetaResultSet getFunctionColumns( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat functionNamePattern, Pat columnNamePattern ) {
        return null;
    }


    @Override
    public MetaResultSet getPseudoColumns( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern ) {
        return null;
    }


    /**
     * Creates an iterable for a result set.
     *
     * <p>The default implementation just returns {@code iterable}, which it
     * requires to be not null; derived classes may instead choose to execute the
     * relational expression in {@code signature}.
     */
    @Override
    public Iterable<Object> createIterable( StatementHandle stmt, QueryState state, Signature signature, List<TypedValue> parameters, Frame firstFrame ) {
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
    public StatementHandle prepare( ConnectionHandle ch, String sql, long maxRowCount ) {
        return null;
    }


    /**
     * Prepares and executes a statement.
     *
     * @param h Statement handle
     * @param sql SQL query
     * @param maxRowCount Negative for no limit (different meaning than JDBC)
     * @param callback Callback to lock, clear and assign cursor
     * @return Result containing statement ID, and if a query, a result set and
     * first frame of data
     * @deprecated See {@link #prepareAndExecute(StatementHandle, String, long, int, PrepareCallback)}
     */
    @Override
    public ExecuteResult prepareAndExecute( StatementHandle h, String sql, long maxRowCount, PrepareCallback callback ) throws NoSuchStatementException {
        return null;
    }


    /**
     * Prepares and executes a statement.
     *
     * @param h Statement handle
     * @param sql SQL query
     * @param maxRowCount Maximum number of rows for the entire query. Negative for no limit
     * (different meaning than JDBC).
     * @param maxRowsInFirstFrame Maximum number of rows for the first frame. This value should
     * always be less than or equal to {@code maxRowCount} as the number of results are guaranteed
     * to be restricted by {@code maxRowCount} and the underlying database.
     * @param callback Callback to lock, clear and assign cursor
     * @return Result containing statement ID, and if a query, a result set and
     * first frame of data
     */
    @Override
    public ExecuteResult prepareAndExecute( StatementHandle h, String sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws NoSuchStatementException {
        return null;
    }


    /**
     * Prepares a statement and then executes a number of SQL commands in one pass.
     *
     * @param h Statement handle
     * @param sqlCommands SQL commands to run
     * @return An array of update counts containing one element for each command in the batch.
     */
    @Override
    public ExecuteBatchResult prepareAndExecuteBatch( StatementHandle h, List<String> sqlCommands ) throws NoSuchStatementException {
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
    public ExecuteBatchResult executeBatch( StatementHandle h, List<List<TypedValue>> parameterValues ) throws NoSuchStatementException {
        return null;
    }


    /**
     * Returns a frame of rows.
     *
     * <p>The frame describes whether there may be another frame. If there is not
     * another frame, the current iteration is done when we have finished the
     * rows in the this frame.
     *
     * <p>The default implementation always returns null.
     *
     * @param h Statement handle
     * @param offset Zero-based offset of first row in the requested frame
     * @param fetchMaxRowCount Maximum number of rows to return; negative means
     * no limit
     * @return Frame, or null if there are no more
     */
    @Override
    public Frame fetch( StatementHandle h, long offset, int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException {
        return null;
    }


    /**
     * Executes a prepared statement.
     *
     * @param h Statement handle
     * @param parameterValues A list of parameter values; may be empty, not null
     * @param maxRowCount Maximum number of rows to return; negative means
     * no limit
     * @return Execute result
     * @deprecated See {@link #execute(StatementHandle, List, int)}
     */
    @Override
    public ExecuteResult execute( StatementHandle h, List<TypedValue> parameterValues, long maxRowCount ) throws NoSuchStatementException {
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
    public ExecuteResult execute( StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) throws NoSuchStatementException {
        return null;
    }


    /**
     * Called during the creation of a statement to allocate a new handle.
     *
     * @param ch Connection handle
     */
    @Override
    public StatementHandle createStatement( ConnectionHandle ch ) {
        return null;
    }


    /**
     * Closes a statement.
     *
     * <p>If the statement handle is not known, or is already closed, does
     * nothing.
     *
     * @param h Statement handle
     */
    @Override
    public void closeStatement( StatementHandle h ) {

    }


    /**
     * Opens (creates) a connection. The client allocates its own connection ID which the server is
     * then made aware of through the {@link ConnectionHandle}. The Map {@code info} argument is
     * analogous to the {@link Properties} typically passed to a "normal" JDBC Driver. Avatica
     * specific properties should not be included -- only properties for the underlying driver.
     *
     * @param ch A ConnectionHandle encapsulates information about the connection to be opened
     * as provided by the client.
     * @param info A Map corresponding to the Properties typically passed to a JDBC Driver.
     */
    @Override
    public void openConnection( ConnectionHandle ch, Map<String, String> info ) {

    }


    /**
     * Closes a connection
     */
    @Override
    public void closeConnection( ConnectionHandle ch ) {

    }


    /**
     * Re-sets the {@link ResultSet} on a Statement. Not a JDBC method.
     *
     * @return True if there are results to fetch after resetting to the given offset. False otherwise
     */
    @Override
    public boolean syncResults( StatementHandle sh, QueryState state, long offset ) throws NoSuchStatementException {
        return false;
    }


    /**
     * Makes all changes since the last commit/rollback permanent. Analogous to
     * {@link Connection#commit()}.
     *
     * @param ch A reference to the real JDBC Connection
     */
    @Override
    public void commit( ConnectionHandle ch ) {

    }


    /**
     * Undoes all changes since the last commit/rollback. Analogous to
     * {@link Connection#rollback()};
     *
     * @param ch A reference to the real JDBC Connection
     */
    @Override
    public void rollback( ConnectionHandle ch ) {

    }


    /**
     * Synchronizes client and server view of connection properties.
     *
     * <p>Note: this interface is considered "experimental" and may undergo further changes as this
     * functionality is extended to other aspects of state management for
     * {@link Connection}, {@link Statement}, and {@link ResultSet}.</p>
     */
    @Override
    public ConnectionProperties connectionSync( ConnectionHandle ch, ConnectionProperties connProps ) {
        return null;
    }


    /**
     * Callback for {@link #connectionCache} member expiration.
     */
    private class ConnectionExpiryHandler implements RemovalListener<String, Connection> {

        public void onRemoval( RemovalNotification<String, Connection> notification ) {
            String connectionId = notification.getKey();
            Connection doomed = notification.getValue();
            LOG.debug( "Expiring connection {} because {}", connectionId, notification.getCause() );
            try {
                if ( doomed != null ) {
                    doomed.close();
                }
            } catch ( Throwable t ) {
                LOG.info( "Exception thrown while expiring connection {}", connectionId, t );
            }
        }
    }


    /**
     * Callback for {@link #statementCache} member expiration.
     */
    private class StatementExpiryHandler implements RemovalListener<Integer, StatementInfo> {

        public void onRemoval( RemovalNotification<Integer, StatementInfo> notification ) {
            Integer stmtId = notification.getKey();
            StatementInfo doomed = notification.getValue();
            if ( doomed == null ) {
                // log/throw?
                return;
            }
            LOG.debug( "Expiring statement {} because {}", stmtId, notification.getCause() );
            try {
                if ( doomed.getResultSet() != null ) {
                    doomed.getResultSet().close();
                }
                if ( doomed.statement != null ) {
                    doomed.statement.close();
                }
            } catch ( Throwable t ) {
                LOG.info( "Exception thrown while expiring statement {}", stmtId, t );
            }
        }
    }
}
