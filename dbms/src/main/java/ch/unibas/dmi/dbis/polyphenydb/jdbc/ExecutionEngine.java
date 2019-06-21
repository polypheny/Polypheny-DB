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


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgram;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.CalcSplitRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterTableScanRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectTableScanRule;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainFormat;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.tools.Planner;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelConversionException;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelRunners;
import ch.unibas.dmi.dbis.polyphenydb.tools.ValidationException;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ExecutionEngine {

    private static final Logger LOG = LoggerFactory.getLogger( ExecutionEngine.class );

    private static ExecutionEngine INSTANCE;


    static {
        INSTANCE = new ExecutionEngine();
    }


    public static ExecutionEngine getInstance() {
        return INSTANCE;
    }


    private ExecutionEngine() {

    }


    public ExecuteResult executeSelect( StatementHandle h, PolyphenyDbStatementHandle statement, int maxRowsInFirstFrame, long maxRowCount, Planner planner, StopWatch stopWatch, SqlNode parsed ) throws NoSuchStatementException {

        //////////////////////
        // (3)  VALIDATION  //
        //////////////////////
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Validating ..." );
        }
        stopWatch.start();
        SqlNode validated;
        try {
            validated = planner.validate( parsed );
        } catch ( ValidationException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }
        stopWatch.stop();
        if ( LOG.isTraceEnabled() ) {
            LOG.debug( "Validated query: [{}]", validated );
        }
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Validating ... done. [{}]", stopWatch );
        }

        /////////////////////////
        // (4)  AUTHORIZATION  //
        /////////////////////////
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Authorizing ..." );
        }
        stopWatch.start();
        // TODO
        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Authorizing ... done. [{}]", stopWatch );
        }

        //////////////////////
        // (5)  Planning    //
        //////////////////////
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Planning ..." );
        }
        stopWatch.start();
        RelNode logicalPlan;
        try {
            logicalPlan = planner.rel( validated ).rel;
        } catch ( RelConversionException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }
        if ( LOG.isTraceEnabled() ) {
            LOG.debug( "Logical query plan: [{}]", RelOptUtil.dumpPlan( "-- Logical Plan", logicalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Planning ... done. [{}]", stopWatch );
        }

        /////////////////////////
        // (5)  Optimization  //
        ///////////////////////
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Optimization ..." );
        }
        stopWatch.start();

        final HepProgram hepProgram =
                new HepProgramBuilder()
                        .addRuleInstance( CalcSplitRule.INSTANCE )
                        .addRuleInstance( FilterTableScanRule.INSTANCE )
                        .addRuleInstance( FilterTableScanRule.INTERPRETER )
                        .addRuleInstance( ProjectTableScanRule.INSTANCE )
                        .addRuleInstance( ProjectTableScanRule.INTERPRETER )
                        .build();
        final HepPlanner hepPlanner = new HepPlanner( hepProgram );
        hepPlanner.setRoot( logicalPlan );
        RelNode optimalPlan = hepPlanner.findBestExp();

        if ( LOG.isTraceEnabled() ) {
            LOG.debug( "Optimized query plan: [{}]", RelOptUtil.dumpPlan( "-- Best Plan", optimalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }

        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Optimization ... done. [{}]", stopWatch );
        }

        /////////////////////
        // (6)  EXECUTION  //
        /////////////////////
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Execution ..." );
        }
        stopWatch.start();

        PolyphenyDbResultSet resultSet;
        try {
            PreparedStatement preparedStatement = RelRunners.run( optimalPlan );
            resultSet = (PolyphenyDbResultSet) preparedStatement.executeQuery();
        } catch ( SQLException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }

        //connection.setCurrentOpenResultSet(resultSet);

        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Execution ... done. [{}]", stopWatch );
        }

        /////////
        // (6.5) TRANSACTION ID
        ////////
        /*if ( rawExecutionResult.subType() == resultSet..Type.TRANSACTION_CONTROL ) {
            // TODO: check whether only in case of success or always (try-finally)
            if ( LOG.isTraceEnabled() ) {
                LOG.trace( "Deleting the current TransactionId: {}", xid );
            }
            connection.endCurrentTransaction();
        }*/

        ///////////////////////
        // (7)  MARSHALLING  //
        ///////////////////////
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Building response ..." );
        }
        stopWatch.start();

        Signature signature;
        try {
            signature = signature( resultSet.getMetaData(), null, parsed.toSqlString( null ).getSql(), StatementType.SELECT );
        } catch ( SQLException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.FATAL );
        }

        List<MetaResultSet> resultSets = Collections.emptyList();
        if ( parsed.isA( SqlKind.QUERY ) ) {
            // ResultSet
            statement.setOpenResultSet( resultSet );
            try {
                resultSets = Collections.singletonList( MetaResultSet.create(
                        h.connectionId,
                        h.id,
                        false,
                        signature,
                        maxRowsInFirstFrame > 0 ? DbmsMeta.getInstance().fetch( h, 0, (int) Math.min( Math.max( maxRowCount, maxRowsInFirstFrame ), Integer.MAX_VALUE ) ) : Frame.MORE
                        //null
                ) );
            } catch ( MissingResultsException e ) {
                throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.FATAL );
            }
        } else {
            // TODO
            throw new RuntimeException( "Implement!" );
        }

        return new ExecuteResult( resultSets );
    }


    public ExecuteResult executeExplain( StatementHandle h, PolyphenyDbStatementHandle statement, int maxRowsInFirstFrame, long maxRowCount, Planner planner, StopWatch stopWatch, SqlNode parsed ) {
        throw new RuntimeException( "Explain is not supported yet!" );
    }


    public ExecuteResult executeDdl( StatementHandle h, PolyphenyDbStatementHandle statement, Planner planner, StopWatch stopWatch, SqlNode parsed ) {
        System.out.println( parsed );
        return null;
    }


    /*
     *
     *  ///// Helpers /////
     *
     */


    /**
     * Converts from JDBC metadata to Avatica columns.
     */
    private static List<ColumnMetaData> columns( final ResultSetMetaData metaData ) throws SQLException {
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
                    new ColumnMetaData(
                            i - 1,
                            metaData.isAutoIncrement( i ),
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
    private static List<AvaticaParameter> parameters( final ParameterMetaData metaData ) throws SQLException {
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


    private static Signature signature( final ResultSetMetaData metaData, final ParameterMetaData parameterMetaData, final String sql, final StatementType statementType ) throws SQLException {
        final CursorFactory cf = CursorFactory.LIST;  // because JdbcResultSet#frame
        return new Signature( columns( metaData ), sql, parameters( parameterMetaData ), null, cf, statementType );
    }


    protected static Signature signature( final ResultSetMetaData metaData ) throws SQLException {
        return signature( metaData, null, null, null );
    }


}
