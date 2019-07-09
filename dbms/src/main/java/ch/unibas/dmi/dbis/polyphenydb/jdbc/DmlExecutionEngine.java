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
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.PolyphenyDbSignature;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgram;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.CalcSplitRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterTableScanRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectTableScanRule;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplain;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainFormat;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelect;
import ch.unibas.dmi.dbis.polyphenydb.tools.Planner;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelConversionException;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelRunners;
import ch.unibas.dmi.dbis.polyphenydb.tools.ValidationException;
import com.google.common.collect.ImmutableList;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta.ConnectionHandle;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.ExecuteResult;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.MetaResultSet;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DmlExecutionEngine {

    private static final Logger LOG = LoggerFactory.getLogger( DmlExecutionEngine.class );

    private static DmlExecutionEngine INSTANCE;


    static {
        INSTANCE = new DmlExecutionEngine();
    }


    public static DmlExecutionEngine getInstance() {
        return INSTANCE;
    }


    private DmlExecutionEngine() {

    }


    public ExecuteResult executeSelect( StatementHandle h, PolyphenyDbStatementHandle statement, int maxRowsInFirstFrame, long maxRowCount, Planner planner, StopWatch stopWatch, SqlSelect parsed ) throws NoSuchStatementException {
        //
        // 3: Validation
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Validating ..." );
        }
        stopWatch.start();
        SqlNode validated = validate( parsed, planner );
        stopWatch.stop();
        if ( LOG.isTraceEnabled() ) {
            LOG.debug( "Validated query: [{}]", validated );
        }
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Validating ... done. [{}]", stopWatch );
        }

        //
        // 4: authorization
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Authorizing ..." );
        }
        stopWatch.start();
        authorize( parsed );
        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Authorizing ... done. [{}]", stopWatch );
        }

        //
        // 5: planning
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Planning ..." );
        }
        stopWatch.start();
        RelNode logicalPlan = plan( validated, planner );
        if ( LOG.isTraceEnabled() ) {
            LOG.debug( "Logical query plan: [{}]", RelOptUtil.dumpPlan( "-- Logical Plan", logicalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Planning ... done. [{}]", stopWatch );
        }

        //
        // 6: optimization
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Optimization ..." );
        }
        stopWatch.start();
        RelNode optimalPlan = optimize( logicalPlan );
        if ( LOG.isTraceEnabled() ) {
            LOG.debug( "Optimized query plan: [{}]", RelOptUtil.dumpPlan( "-- Best Plan", optimalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Optimization ... done. [{}]", stopWatch );
        }

        //
        // 7: execution
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Execution ..." );
        }
        stopWatch.start();
        PolyphenyDbResultSet resultSet = execute( optimalPlan );
        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Execution ... done. [{}]", stopWatch );
        }

        //
        // (7.5) transaction id
        /*if ( rawExecutionResult.subType() == resultSet..Type.TRANSACTION_CONTROL ) {
            // TODO: check whether only in case of success or always (try-finally)
            if ( LOG.isTraceEnabled() ) {
                LOG.trace( "Deleting the current TransactionId: {}", xid );
            }
            connection.endCurrentTransaction();
        }*/

        //
        // 8:  marshalling
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

        List<MetaResultSet> resultSets;
        statement.setOpenResultSet( resultSet );
        try {
            resultSets = Collections.singletonList( MetaResultSet.create(
                    h.connectionId,
                    h.id,
                    false,
                    signature,
                    maxRowsInFirstFrame > 0 ? DbmsMeta.getInstance().fetch( h, 0, (int) Math.min( Math.max( maxRowCount, maxRowsInFirstFrame ), Integer.MAX_VALUE ) ) : Frame.MORE // Send first frame to together with the response to save a fetch call
            ) );
        } catch ( MissingResultsException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.FATAL );
        }

        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Building response ... done. [{}]", stopWatch );
        }

        return new ExecuteResult( resultSets );
    }


    public ExecuteResult executeDml( final StatementHandle h, final PolyphenyDbStatementHandle statement, final int maxRowsInFirstFrame, final long maxRowCount, final Planner planner, final StopWatch stopWatch, final SqlNode parsed ) {

        //
        // 3: Validation
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Validating ..." );
        }
        stopWatch.start();
        SqlNode validated = validate( parsed, planner );
        stopWatch.stop();
        if ( LOG.isTraceEnabled() ) {
            LOG.debug( "Validated query: [{}]", validated );
        }
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Validating ... done. [{}]", stopWatch );
        }


        // TODO: Implement DML support

        switch ( parsed.getKind() ) {
            case INSERT:
                break;

            case DELETE:
                break;

            case UPDATE:

            case MERGE:
            case PROCEDURE_CALL:

            default:
                throw new RuntimeException( "Unknown or unsupported dml query type: " + parsed.getKind().name() );
        }

        return null;
    }


    public ExecuteResult explain( final StatementHandle h, final PolyphenyDbStatementHandle statement, final Planner planner, final StopWatch stopWatch, final SqlExplain explainQuery ) {

        SqlNode explicandum = explainQuery.getExplicandum();

        // 3: validation
        SqlNode validated = validate( explicandum, planner );

        // 4: authorization
        authorize( explicandum );

        // 5: planning
        RelNode logicalPlan = plan( validated, planner );

        // 6: optimization
        RelNode optimalPlan = optimize( logicalPlan );

        // 7: explain
        String explanation;
        if ( explainQuery.withImplementation() ) { // Physical Plan
            throw new RuntimeException( "Getting physical query plan is not implemented yet!" );
        } else if ( explainQuery.withType() ) { // Type
            throw new RuntimeException( "Getting type is not implemented yet!" );
        } else { // Logical plan
            explanation = RelOptUtil.dumpPlan( "", optimalPlan, explainQuery.getFormat(), explainQuery.getDetailLevel() );
        }

        // 7: marshalling
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Building response ..." );
        }
        stopWatch.start();

        LinkedList<MetaResultSet> resultSets = new LinkedList<>();
        final Enumerable<Object> enumerable = Linq4j.singletonEnumerable( new String[]{ explanation } );

        final List<ColumnMetaData> columns = new ArrayList<>();
        columns.add( MetaImpl.columnMetaData( "plan", 1, String.class, false ) );
        resultSets.add( createMetaResultSet( statement.getConnection().getHandle(), h, Collections.emptyMap(), columns, CursorFactory.LIST, new Frame( 0, true, enumerable ) ) );

        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Building response ... done. [{}]", stopWatch );
        }

        return new ExecuteResult( resultSets );
    }


    private SqlNode validate( final SqlNode parsed, final Planner planner ) {
        SqlNode validated;
        try {
            validated = planner.validate( parsed );
        } catch ( ValidationException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }
        return validated;
    }


    private void authorize( final SqlNode validated ) {
        // TODO
    }


    private RelNode plan( final SqlNode validated, final Planner planner ) {
        RelNode logicalPlan;
        try {
            logicalPlan = planner.rel( validated ).rel;
        } catch ( RelConversionException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }

        return logicalPlan;
    }


    private RelNode optimize( final RelNode logicalPlan ) {
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
        return hepPlanner.findBestExp();
    }


    private PolyphenyDbResultSet execute( final RelNode optimalPlan ) {
        PolyphenyDbResultSet resultSet;
        try {
            PreparedStatement preparedStatement = RelRunners.run( optimalPlan );
            resultSet = (PolyphenyDbResultSet) preparedStatement.executeQuery();
        } catch ( SQLException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }

        //connection.setCurrentOpenResultSet(resultSet);
        return resultSet;
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


}
