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


import ch.unibas.dmi.dbis.polyphenydb.PUID;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.config.PolyphenyDbConnectionConfig;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollations;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation.Direction;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelFieldCollation.NullDirection;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schema.TableType;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.Statistic;
import ch.unibas.dmi.dbis.polyphenydb.schema.Statistics;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.scu.catalog.CatalogManagerImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParseException;
import ch.unibas.dmi.dbis.polyphenydb.tools.FrameworkConfig;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.tools.Planner;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelConversionException;
import ch.unibas.dmi.dbis.polyphenydb.tools.ValidationException;
import ch.unibas.dmi.dbis.polyphenydb.usermanagement.AuthenticationException;
import ch.unibas.dmi.dbis.polyphenydb.usermanagement.Authenticator;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DbmsMeta implements Meta {

    private static final Logger LOGGER = LoggerFactory.getLogger( DbmsMeta.class );
    private static final DbmsMeta INSTANCE = new DbmsMeta();

    private static final ConcurrentMap<String, ch.unibas.dmi.dbis.polyphenydb.jdbc.ConnectionHandle> OPEN_CONNECTIONS_OLD = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, ch.unibas.dmi.dbis.polyphenydb.jdbc.StatementHandle> OPEN_STATEMENTS_OLD = new ConcurrentHashMap<>();


    public static final DbmsMeta instance() {
        return INSTANCE;
    }


    public static final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
    public SchemaPlus defaultSchema = Frameworks.createRootSchema( true );

    private FrameworkConfig config;
    private Planner planner;


    private DbmsMeta() {
        defaultSchema.add( "ORDER_DETAILS", new Table() {
            public RelDataType getRowType( RelDataTypeFactory typeFactory ) {
                RelDataTypeFactory.FieldInfoBuilder b = typeFactory.builder();
                b.add( "CK_TIME", typeFactory.createJavaType( Date.class ) );
                b.add( "ITEM_ID", typeFactory.createJavaType( Long.class ) );
                b.add( "ITEM_PRICE", typeFactory.createJavaType( Double.class ) );
                b.add( "BUYER_NAME", typeFactory.createJavaType( String.class ) );
                b.add( "QUANTITY", typeFactory.createJavaType( Integer.class ) );

                return b.build();
            }


            public Statistic getStatistic() {
//        return Statistics.of(100, ImmutableList.<ImmutableBitSet>of());
                Direction dir = Direction.ASCENDING;
                RelFieldCollation collation = new RelFieldCollation( 0, dir, NullDirection.UNSPECIFIED );
                return Statistics.of( 5, ImmutableList.of( ImmutableBitSet.of( 0 ) ),
                        ImmutableList.of( RelCollations.of( collation ) ) );
            }


            public TableType getJdbcTableType() {
                return TableType.STREAM;
            }


            @Override
            public boolean isRolledUp( String column ) {
                return false;
            }


            @Override
            public boolean rolledUpColumnValidInsideAgg( String column, SqlCall call, SqlNode parent, PolyphenyDbConnectionConfig config ) {
                return false;
            }


            public Table stream() {
                return null;
            }

        } );

        config = Frameworks.newConfigBuilder().defaultSchema( defaultSchema ).build();
        planner = Frameworks.getPlanner( config );
    }


    @Override
    public Map<DatabaseProperty, Object> getDatabaseProperties( ConnectionHandle ch ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getDatabaseProperties( ConnectionHandle {} )", ch );
        }

        Map<DatabaseProperty, Object> databaseProperties = new HashMap<>();
        for ( DatabaseProperty property : DatabaseProperty.values() ) {
            switch ( property ) {
                case GET_S_Q_L_KEYWORDS:
                    // TODO
                    break;
                case GET_NUMERIC_FUNCTIONS:
                    // TODO
                    break;
                case GET_STRING_FUNCTIONS:
                    // TODO
                    break;
                case GET_SYSTEM_FUNCTIONS:
                    // TODO
                    break;
                case GET_TIME_DATE_FUNCTIONS:
                    // TODO
                    break;
            }
        }

        LOGGER.warn( "[NOT PROPERLY IMPLEMENTED YET] getDatabaseProperties( ConnectionHandle {} )", ch );
        return Collections.unmodifiableMap( databaseProperties );
    }


    @Override
    public MetaResultSet getCatalogs( ConnectionHandle ch ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getCatalogs( ConnectionHandle {} )", ch );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getCatalogs( ConnectionHandle {} )", ch );
        return null;
    }


    @Override
    public MetaResultSet getSchemas( ConnectionHandle ch, String catalog, Pat schemaPattern ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getSchemas( ConnectionHandle {}, String {}, Pat {} )", ch, catalog, schemaPattern );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getSchemas( ConnectionHandle {}, String {}, Pat {} )", ch, catalog, schemaPattern );
        return null;
    }


    @Override
    public MetaResultSet getTables( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, List<String> typeList ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getTables( ConnectionHandle {}, String {}, Pat {}, Pat {}, List<String> {} )", ch, catalog, schemaPattern, tableNamePattern, typeList );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getTables( ConnectionHandle {}, String {}, Pat {}, Pat {}, List<String> {} )", ch, catalog, schemaPattern, tableNamePattern, typeList );
        return null;
    }


    @Override
    public MetaResultSet getColumns( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern, columnNamePattern );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern, columnNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getTableTypes( ConnectionHandle ch ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getTableTypes( ConnectionHandle {} )", ch );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getTableTypes( ConnectionHandle {} )", ch );
        return null;
    }


    @Override
    public MetaResultSet getProcedures( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat procedureNamePattern ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getProcedures( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getProcedures( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getProcedureColumns( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat procedureNamePattern, Pat columnNamePattern ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getProcedureColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern, columnNamePattern );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getProcedureColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, procedureNamePattern, columnNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getColumnPrivileges( ConnectionHandle ch, String catalog, String schema, String table, Pat columnNamePattern ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getColumnPrivileges( ConnectionHandle {}, String {}, String {}, String {}, Pat {} )", ch, catalog, schema, table, columnNamePattern );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getColumnPrivileges( ConnectionHandle {}, String {}, String {}, String {}, Pat {} )", ch, catalog, schema, table, columnNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getTablePrivileges( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getTablePrivileges( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getTablePrivileges( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getBestRowIdentifier( ConnectionHandle ch, String catalog, String schema, String table, int scope, boolean nullable ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getBestRowIdentifier( ConnectionHandle {}, String {}, String {}, String {}, int {}, boolean {} )", ch, catalog, schema, table, scope, nullable );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getBestRowIdentifier( ConnectionHandle {}, String {}, String {}, String {}, int {}, boolean {} )", ch, catalog, schema, table, scope, nullable );
        return null;
    }


    @Override
    public MetaResultSet getVersionColumns( ConnectionHandle ch, String catalog, String schema, String table ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getVersionColumns( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getVersionColumns( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        return null;
    }


    @Override
    public MetaResultSet getPrimaryKeys( ConnectionHandle ch, String catalog, String schema, String table ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getPrimaryKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getPrimaryKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        return null;
    }


    @Override
    public MetaResultSet getImportedKeys( ConnectionHandle ch, String catalog, String schema, String table ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getImportedKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getImportedKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        return null;
    }


    @Override
    public MetaResultSet getExportedKeys( ConnectionHandle ch, String catalog, String schema, String table ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getExportedKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getExportedKeys( ConnectionHandle {}, String {}, String {}, String {} )", ch, catalog, schema, table );
        return null;
    }


    @Override
    public MetaResultSet getCrossReference( ConnectionHandle ch, String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getCrossReference( ConnectionHandle {}, String {}, String {}, String {}, String {}, String {}, String {} )", ch, parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getCrossReference( ConnectionHandle {}, String {}, String {}, String {}, String {}, String {}, String {} )", ch, parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable );
        return null;
    }


    @Override
    public MetaResultSet getTypeInfo( ConnectionHandle ch ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getTypeInfo( ConnectionHandle {} )", ch );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getTypeInfo( ConnectionHandle {} )", ch );
        return null;
    }


    @Override
    public MetaResultSet getIndexInfo( ConnectionHandle ch, String catalog, String schema, String table, boolean unique, boolean approximate ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getIndexInfo( ConnectionHandle {}, String {}, String {}, String {}, boolean {}, boolean {} )", ch, catalog, schema, table, unique, approximate );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getIndexInfo( ConnectionHandle {}, String {}, String {}, String {}, boolean {}, boolean {} )", ch, catalog, schema, table, unique, approximate );
        return null;
    }


    @Override
    public MetaResultSet getUDTs( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern, int[] types ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getUDTs( ConnectionHandle {}, String {}, Pat {}, Pat {}, int[] {} )", ch, catalog, schemaPattern, typeNamePattern, types );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getUDTs( ConnectionHandle {}, String {}, Pat {}, Pat {}, int[] {} )", ch, catalog, schemaPattern, typeNamePattern, types );
        return null;
    }


    @Override
    public MetaResultSet getSuperTypes( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getSuperTypes( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getSuperTypes( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getSuperTables( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getSuperTables( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getSuperTables( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getAttributes( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat typeNamePattern, Pat attributeNamePattern ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getAttributes( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern, attributeNamePattern );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getAttributes( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, typeNamePattern, attributeNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getClientInfoProperties( ConnectionHandle ch ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getClientInfoProperties( ConnectionHandle {} )", ch );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getClientInfoProperties( ConnectionHandle {} )", ch );
        return null;
    }


    @Override
    public MetaResultSet getFunctions( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat functionNamePattern ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getFunctions( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getFunctions( ConnectionHandle {}, String {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getFunctionColumns( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat functionNamePattern, Pat columnNamePattern ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getFunctionColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern, columnNamePattern );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getFunctionColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, functionNamePattern, columnNamePattern );
        return null;
    }


    @Override
    public MetaResultSet getPseudoColumns( ConnectionHandle ch, String catalog, Pat schemaPattern, Pat tableNamePattern, Pat columnNamePattern ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "getPseudoColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern, columnNamePattern );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] getPseudoColumns( ConnectionHandle {}, String {}, Pat {}, Pat {}, Pat {} )", ch, catalog, schemaPattern, tableNamePattern, columnNamePattern );
        return null;
    }


    @Override
    public Iterable<Object> createIterable( StatementHandle stmt, QueryState state, Signature signature, List<TypedValue> parameters, Frame firstFrame ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "createIterable( StatementHandle {}, QueryState {}, Signature {}, List<TypedValue> {}, Frame {} )", stmt, state, signature, parameters, firstFrame );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] createIterable( StatementHandle {}, QueryState {}, Signature {}, List<TypedValue> {}, Frame {} )", stmt, state, signature, parameters, firstFrame );
        return null;
    }


    @Override
    public StatementHandle prepare( final ConnectionHandle connectionHandle, final String sql, final long maxRowCount ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepare( ConnectionHandle {}, String {}, long {} )", connectionHandle, sql, maxRowCount );
        }
/*
        final String connectionId = connectionHandle.id;
        final int statementId = 0;

        final StatementHandle preparedStatement = new StatementHandle( connectionId, statementId, prepare( sql, maxRowCount ) );
*/
        LOGGER.error( "[NOT IMPLEMENTED YET] prepare( ConnectionHandle {}, String {}, long {} )", connectionHandle, sql, maxRowCount );
        return null;
    }


    protected Signature prepare( final String sql, final long maxRowCount ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepare( String {}, long {} )", sql, maxRowCount );
        }

        final StopWatch stopWatch = new StopWatch();
        final PUID statementId = null;//statement.getStatementId();

        if ( LOGGER.isDebugEnabled() ) {
            LOGGER.debug( "Parsing PolySQL statement ..." );
        }


        /*
        stopWatch.start();

        final ParseResult parseResult;
        try {
            parseResult = QueryParser.getComponent().parse( ParseJob.newParseJob( statementId, sql ) );
        } catch ( ParsingException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }
        stopWatch.stop();
        if ( LOGGER.isDebugEnabled() ) {
            LOGGER.debug( "Parsing PolySQL statement ... done. [{}]", stopWatch );
        }
        assert parseResult != null : "The Query Parser Module returned null on parse(request) and this was not proper handled!";
*/

        final List<ColumnMetaData> columns = Collections.emptyList();
        final List<AvaticaParameter> parameters = Collections.emptyList();
        final Map<String, Object> internalParameters = Collections.emptyMap();
        final CursorFactory cursorFactory = CursorFactory.LIST;
        final StatementType statementType = StatementType.SELECT;

        final Signature signature = new Signature( columns, sql, parameters, internalParameters, cursorFactory, statementType );

        return signature;
    }


    @Override
    public ExecuteResult prepareAndExecute( final StatementHandle statementHandle, final String sql, final long maxRowCount, final PrepareCallback callback ) throws NoSuchStatementException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecute( StatementHandle {}, String {}, long {}, PrepareCallback {} )", statementHandle, sql, maxRowCount, callback );
        }

        return prepareAndExecute( statementHandle, sql, maxRowCount, -1, callback );
    }


    @Override
    public ExecuteResult prepareAndExecute( StatementHandle statementHandle, String sql, long maxRowCount, int maxRowsInFirstFrame, PrepareCallback callback ) throws NoSuchStatementException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecute( StatementHandle {}, String {}, long {}, int {}, PrepareCallback {} )", statementHandle, sql, maxRowCount, maxRowsInFirstFrame, callback );
        }
        final StopWatch stopWatch = new StopWatch();

        /*
        final ch.unibas.dmi.dbis.polyphenydb.jdbc.ConnectionHandle connection = OPEN_CONNECTIONS_OLD.get( statementHandle.connectionId );
        final ch.unibas.dmi.dbis.polyphenydb.jdbc.StatementHandle statement;
        synchronized ( OPEN_STATEMENTS_OLD ) {
            if ( OPEN_STATEMENTS_OLD.containsKey( statementHandle.connectionId + "::" + Integer.toString( statementHandle.id ) ) ) {
                statement = OPEN_STATEMENTS_OLD.get( statementHandle.connectionId + "::" + Integer.toString( statementHandle.id ) );
            } else {
                statement = new StatementImpl( connection, StatementId.fromInt( statementHandle.id ) );
                OPEN_STATEMENTS_OLD.put( statementHandle.connectionId + "::" + Integer.toString( statementHandle.id ), statement );
            }
        }*/

        SqlNode parse;
        try {
            parse = planner.parse( sql );
            System.out.println( "parse>" + parse.toString() );
        } catch ( SqlParseException e ) {
            throw new RuntimeException( "parse failed: " + e.getMessage(), e );
        }

        SqlNode validate = null;
        try {
            validate = planner.validate( parse );
            System.out.println( "validate>" + validate );
        } catch ( ValidationException e ) {
            throw new RuntimeException( "validate failed: " + e.getMessage(), e );
        } catch ( Throwable t ) {
            t.printStackTrace();
        }

        RelNode tree;
        try {
            tree = planner.convert( validate );
            System.out.println( "tree>" + validate );
        } catch ( RelConversionException e ) {
            throw new RuntimeException( "convert failed: " + e.getMessage(), e );
        }

        String plan = RelOptUtil.toString( tree ); //explain(tree, SqlExplainLevel.ALL_ATTRIBUTES);
        System.out.println( "plan>" );
        System.out.println( plan );



        return null;
    }


    @Override
    public ExecuteBatchResult prepareAndExecuteBatch( StatementHandle h, List<String> sqlCommands ) throws NoSuchStatementException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "prepareAndExecuteBatch( StatementHandle {}, List<String> {} )", h, sqlCommands );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] prepareAndExecuteBatch( StatementHandle {}, List<String> {} )", h, sqlCommands );
        return null;
    }


    @Override
    public ExecuteBatchResult executeBatch( StatementHandle h, List<List<TypedValue>> parameterValues ) throws NoSuchStatementException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "executeBatch( StatementHandle {}, List<List<TypedValue>> {} )", h, parameterValues );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] executeBatch( StatementHandle {}, List<List<TypedValue>> {} )", h, parameterValues );
        return null;
    }


    @Override
    public Frame fetch( final StatementHandle statementHandle, long offset, int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "fetch( StatementHandle {}, long {}, int {} )", statementHandle, offset, fetchMaxRowCount );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET]  fetch( StatementHandle {}, long {}, int {} )", statementHandle, offset, fetchMaxRowCount );
        return null;
    }


    @Override
    public ExecuteResult execute( StatementHandle h, List<TypedValue> parameterValues, long maxRowCount ) throws NoSuchStatementException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "execute( StatementHandle {}, List<TypedValue> {}, long {} )", h, parameterValues, maxRowCount );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] execute( StatementHandle {}, List<TypedValue> {}, long {} )", h, parameterValues, maxRowCount );
        return null;
    }


    @Override
    public ExecuteResult execute( StatementHandle h, List<TypedValue> parameterValues, int maxRowsInFirstFrame ) throws NoSuchStatementException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "execute( StatementHandle {}, List<TypedValue> {}, int {} )", h, parameterValues, maxRowsInFirstFrame );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] execute( StatementHandle {}, List<TypedValue> {}, int {} )", h, parameterValues, maxRowsInFirstFrame );
        return null;
    }


    @Override
    public StatementHandle createStatement( ConnectionHandle ch ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "createStatement( ConnectionHandle {} )", ch );
        }

        final String connectionId = ch.id;
        final int statementId = 0; // TODO: We need a correct statement id
        final StatementHandle statementHandle = new StatementHandle( connectionId, statementId, null );

        return statementHandle;
    }


    @Override
    public void closeStatement( final StatementHandle statementHandle ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "closeStatement( StatementHandle {} )", statementHandle );
        }

        // TODO:
        final ch.unibas.dmi.dbis.polyphenydb.jdbc.StatementHandle toClose = OPEN_STATEMENTS_OLD.remove( statementHandle.connectionId + "::" + Integer.toString( statementHandle.id ) );
        if ( toClose != null ) {
            toClose.setOpenResultSet( null ); // closes the currently open ResultSet
        }
    }


    @Override
    public boolean syncResults( StatementHandle sh, QueryState state, long offset ) throws NoSuchStatementException {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "createStatement( StatementHandle {}, QueryState {}, long {} )", sh, state, offset );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] createStatement( StatementHandle {}, QueryState {}, long {} )", sh, state, offset );
        return false;
    }


    @Override
    public void commit( final ConnectionHandle connectionHandle ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "commit( ConnectionHandle {} )", connectionHandle );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] commit( ConnectionHandle {} )", connectionHandle );
    }


    @Override
    public void rollback( final ConnectionHandle connectionHandle ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "rollback( ConnectionHandle {} )", connectionHandle );
        }

        LOGGER.error( "[NOT IMPLEMENTED YET] rollback( ConnectionHandle {} )", connectionHandle );
    }


    @Override
    public void openConnection( final ConnectionHandle connectionHandle, final Map<String, String> connectionParameters ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "openConnection( ConnectionHandle {}, Map<String, String> {} )", connectionHandle, connectionParameters );
        }

        final ch.unibas.dmi.dbis.polyphenydb.jdbc.ConnectionHandle connectionToOpen;
        synchronized ( OPEN_CONNECTIONS_OLD ) {
            if ( OPEN_CONNECTIONS_OLD.containsKey( connectionHandle.id ) ) {
                if ( LOGGER.isDebugEnabled() ) {
                    LOGGER.debug( "Key {} is already present in the OPEN_CONNECTIONS map.", connectionHandle.id );
                }
                throw new IllegalStateException( "Forbidden attempt to open the connection `" + connectionHandle.id + "` twice!" );
            }

            if ( LOGGER.isDebugEnabled() ) {
                LOGGER.debug( "Creating a new connection." );
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

            final Catalog catalog = CatalogManagerImpl.getInstance().getCatalog();
            // Check database access
            final CatalogDatabase database;
            try {
                database = catalog.getDatabaseFromName( databaseName );
            } catch ( UnknownDatabaseException e ) {
                throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
            }
            assert database != null;

            // final NodeId nodeId = NodeId.fromString( DataDistributionUnit.getComponent().getNodeId().toString() ); // TODO: get real node id -- configuration.get("nodeid")
            // final UserId userId = UserId.fromString( user.getUserId().toString() ); // TODO: get real user id -- connectionParameters.get("user")

            //connectionToOpen = new ConnectionImpl( connectionHandle, 0, user, connectionHandle.id, database, schema );
            connectionToOpen = new ConnectionImpl( connectionHandle, connectionHandle.id );
            // TODO: also store the connectionParameters

            OPEN_CONNECTIONS_OLD.put( connectionHandle.id, connectionToOpen );
        }

        return;
    }


    @Override
    public ConnectionProperties connectionSync( final ConnectionHandle connectionHandle, final ConnectionProperties connProps ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "connectionSync( ConnectionHandle {}, ConnectionProperties {} )", connectionHandle, connProps );
        }

        final ch.unibas.dmi.dbis.polyphenydb.jdbc.ConnectionHandle connectionToSync = OPEN_CONNECTIONS_OLD.get( connectionHandle.id );
        if ( connectionToSync == null ) {
            if ( LOGGER.isDebugEnabled() ) {
                LOGGER.debug( "Connection {} is not open.", connectionHandle.id );
            }
            throw new IllegalStateException( "Attempt to synchronize the connection `" + connectionHandle.id + "` with is either has not been open yet or is already closed." );
        }

//        LOGGER.error( "[NOT IMPLEMENTED YET] connectionSync( ConnectionHandle {}, ConnectionProperties {} )", connectionHandle, connProps );
        return connectionToSync.mergeConnectionProperties( connProps );
    }


    @Override
    public void closeConnection( final ConnectionHandle connectionHandle ) {
        if ( LOGGER.isTraceEnabled() ) {
            LOGGER.trace( "closeConnection( ConnectionHandle {} )", connectionHandle );
        }

        final ch.unibas.dmi.dbis.polyphenydb.jdbc.ConnectionHandle connectionToClose = OPEN_CONNECTIONS_OLD.remove( connectionHandle.id );
        if ( connectionToClose == null ) {
            if ( LOGGER.isDebugEnabled() ) {
                LOGGER.debug( "Connection {} already closed.", connectionHandle.id );
            }
            return;
        }

        synchronized ( OPEN_STATEMENTS_OLD ) {
            for ( final String key : OPEN_STATEMENTS_OLD.keySet() ) {
                if ( key.startsWith( connectionHandle.id ) ) {
                    OPEN_STATEMENTS_OLD.remove( key ).setOpenResultSet( null );
                }
            }
        }

        // TODO: release all resources associated with this connection
        LOGGER.error( "[NOT IMPLEMENTED YET] closeConnection( ConnectionHandle {} )", connectionHandle );
    }

}
