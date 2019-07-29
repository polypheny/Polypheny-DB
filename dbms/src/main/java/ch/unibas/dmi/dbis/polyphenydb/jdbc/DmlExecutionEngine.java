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
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableCalc;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableInterpretable;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel.Prefer;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Interpreters;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.PolyphenyDbSignature;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgram;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelRoot;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.CalcSplitRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.FilterTableScanRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.ProjectTableScanRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import ch.unibas.dmi.dbis.polyphenydb.runtime.ArrayBindable;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Bindable;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplain;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainFormat;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.PolyphenyDbSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ExtraSqlTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.tools.Planner;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelConversionException;
import ch.unibas.dmi.dbis.polyphenydb.tools.ValidationException;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Type;
import java.sql.DatabaseMetaData;
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
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.apache.calcite.linq4j.BaseQueryable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.tree.Expression;
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


    public ExecuteResult executeSelect( final StatementHandle h, final PolyphenyDbStatementHandle statement, int maxRowsInFirstFrame, long maxRowCount, final Planner planner, final StopWatch stopWatch, final PolyphenyDbSchema rootSchema, final SqlSelect parsed ) throws NoSuchStatementException {
        //
        // 3: Validation
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Validating SELECT Statement ..." );
        }
        stopWatch.start();
        Pair<SqlNode, RelDataType> validatePair = validate( parsed, planner );
        SqlNode validated = validatePair.left;
        RelDataType type = validatePair.right;

        stopWatch.stop();
        if ( LOG.isTraceEnabled() ) {
            LOG.debug( "Validated query: [{}]", validated );
        }
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Validating SELECT Statement ... done. [{}]", stopWatch );
        }

        //
        // 4: authorization
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Authorizing SELECT Statement ..." );
        }
        stopWatch.start();
        authorize( parsed );
        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Authorizing SELECT Statement ... done. [{}]", stopWatch );
        }

        //
        // 5: planning
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Planning SELECT Statement ..." );
        }
        stopWatch.start();
        RelNode logicalPlan = plan( validated, planner );
        if ( LOG.isTraceEnabled() ) {
            LOG.debug( "Logical query plan: [{}]", RelOptUtil.dumpPlan( "-- Logical Plan", logicalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        InformationManager.getInstance().addQueryPlan( "LogicalQueryPlan", RelOptUtil.dumpPlan( "", logicalPlan, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Planning SELECT Statement ... done. [{}]", stopWatch );
        }

        //
        // 6: optimization
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Optimizing SELECT Statement ..." );
        }
        stopWatch.start();
        RelNode optimalPlan = optimize( logicalPlan );
        if ( LOG.isTraceEnabled() ) {
            LOG.debug( "Optimized query plan: [{}]", RelOptUtil.dumpPlan( "-- Best Plan", optimalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        InformationManager.getInstance().addQueryPlan( "PhysicalQueryPlan", RelOptUtil.dumpPlan( "", optimalPlan, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Optimizing SELECT Statement ... done. [{}]", stopWatch );
        }

        //
        // 7: prepare to be executed
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Execution SELECT Statement ..." );
        }
        stopWatch.start();
        PolyphenyDbSignature signature = prepareSelect( optimalPlan, statement, rootSchema, parsed.toSqlString( PolyphenyDbSqlDialect.DEFAULT ).getSql(), type );
        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Execution SELECT Statement ... done. [{}]", stopWatch );
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

        statement.setSignature( signature );

        List<MetaResultSet> resultSets;
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


    public ExecuteResult executeDml( final StatementHandle h, final PolyphenyDbStatementHandle statement, final Planner planner, final StopWatch stopWatch, final PolyphenyDbSchema rootSchema, final SqlNode parsed, ContextImpl prepareContext ) {

        //
        // 3: Validation
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Validating DML Statement ..." );
        }
        stopWatch.start();
        Pair<SqlNode, RelDataType> validatePair = validate( parsed, planner );
        SqlNode validated = validatePair.left;

        stopWatch.stop();
        if ( LOG.isTraceEnabled() ) {
            LOG.debug( "Validated query: [{}]", validated );
        }
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Validating DML Statement ... done. [{}]", stopWatch );
        }

        //
        // 4: authorization
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Authorizing DML Statement ..." );
        }
        stopWatch.start();
        authorize( parsed );
        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Authorizing DML Statement ... done. [{}]", stopWatch );
        }

        //
        // 5: planning
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Planning DML Statement ..." );
        }
        stopWatch.start();
        RelNode logicalPlan = plan( validated, planner );
        if ( LOG.isTraceEnabled() ) {
            LOG.debug( "Logical query plan: [{}]", RelOptUtil.dumpPlan( "-- Logical Plan", logicalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        InformationManager.getInstance().addQueryPlan( "LogicalQueryPlan", RelOptUtil.dumpPlan( "", logicalPlan, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Planning DML Statement ... done. [{}]", stopWatch );
        }

        //
        // 6: optimization
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Optimizing DML Statement  ..." );
        }
        stopWatch.start();
        RelNode optimalPlan;
        try {
            //optimalPlan = planner.transform( 0, planner.getEmptyTraitSet().replace( StoreManager.getInstance().getStore( 0 ).getConvention() ), logicalPlan );
            optimalPlan = planner.transform( 0, planner.getEmptyTraitSet().replace( EnumerableConvention.INSTANCE ), logicalPlan );
        } catch ( RelConversionException e ) {
            throw new RuntimeException( e );
        }
        if ( LOG.isTraceEnabled() ) {
            LOG.debug( "Optimized query plan: [{}]", RelOptUtil.dumpPlan( "-- Best Plan", optimalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        InformationManager.getInstance().addQueryPlan( "PhysicalQueryPlan", RelOptUtil.dumpPlan( "", optimalPlan, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Optimizing DML Statement ... done. [{}]", stopWatch );
        }

        //
        // 7: prepare to be executed
        stopWatch.reset();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Prepare to execute DML Statement ..." );
        }
        stopWatch.start();

        RelRoot root = RelRoot.of( optimalPlan, parsed.getKind() );
        EnumerableRel enumerable = (EnumerableRel) root.rel;
        if ( !root.isRefTrivial() ) {
            final List<RexNode> projects = new ArrayList<>();
            final RexBuilder rexBuilder = enumerable.getCluster().getRexBuilder();
            for ( int field : Pair.left( root.fields ) ) {
                projects.add( rexBuilder.makeInputRef( enumerable, field ) );
            }
            RexProgram program = RexProgram.create( enumerable.getRowType(), projects, null, root.validatedRowType, rexBuilder );
            enumerable = EnumerableCalc.create( enumerable, program );
        }

        Bindable bindable = EnumerableInterpretable.toBindable( ImmutableMap.of(), null, enumerable, Prefer.ARRAY );
        Enumerable e = bindable.bind( statement.getDataContext( rootSchema ) );

        stopWatch.stop();
        if ( LOG.isDebugEnabled() ) {
            LOG.debug( "Prepare to execute DML Statement ... done. [{}]", stopWatch );
        }


        MetaResultSet metaResultSet = MetaResultSet.count( h.connectionId, h.id, ((Number) e.iterator().next()).intValue() );

        LinkedList<MetaResultSet> resultSets = new LinkedList<>();
        resultSets.add( metaResultSet );

        return new ExecuteResult( resultSets );
    }


    public ExecuteResult explain( final StatementHandle h, final PolyphenyDbStatementHandle statement, final Planner planner, final StopWatch stopWatch, final SqlExplain explainQuery ) {

        SqlNode explicandum = explainQuery.getExplicandum();

        // 3: validation
        Pair<SqlNode, RelDataType> validatePair = validate( explicandum, planner );
        SqlNode validated = validatePair.left;
        RelDataType type = validatePair.right;

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


    private Pair<SqlNode, RelDataType> validate( final SqlNode parsed, final Planner planner ) {
        Pair<SqlNode, RelDataType> validated;
        try {
            validated = planner.validateAndGetType( parsed );
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


    private PolyphenyDbSignature prepareSelect( final RelNode optimalPlan, final PolyphenyDbStatementHandle statementHandle, final PolyphenyDbSchema rootSchema, final String sql, RelDataType type ) {
        ArrayBindable bindable = Interpreters.bindable( optimalPlan );
        final CursorFactory cf = CursorFactory.ARRAY;
        final JavaTypeFactory typeFactory = statementHandle.getTypeFactory();
        final List<List<String>> origins = Collections.nCopies( optimalPlan.getRowType().getFieldCount(), null );
        List<ColumnMetaData> columnMetaData = getColumnMetaDataList( typeFactory, optimalPlan.getRowType(), optimalPlan.getRowType(), origins );

        final List<AvaticaParameter> parameters = new ArrayList<>();
        final RelDataType parameterRowType = optimalPlan.getRowType();
        for ( RelDataTypeField field : parameterRowType.getFieldList() ) {
            RelDataType fieldType = field.getType();
            parameters.add(
                    new AvaticaParameter(
                            false,
                            getPrecision( fieldType ),
                            getScale( fieldType ),
                            getTypeOrdinal( fieldType ),
                            getTypeName( fieldType ),
                            "",
                            field.getName() ) );
        }

        RelDataType jdbcType = type;
        if ( !type.isStruct() ) {
            jdbcType = statementHandle.getTypeFactory().builder().add( "$0", type ).build();
        }

        return new PolyphenyDbSignature( sql, parameters, ImmutableMap.of(), jdbcType, columnMetaData, cf, rootSchema, ImmutableList.of(), -1, bindable, StatementType.SELECT );
    }


    private List<ColumnMetaData> getColumnMetaDataList( JavaTypeFactory typeFactory, RelDataType x, RelDataType jdbcType, List<List<String>> originList ) {
        final List<ColumnMetaData> columns = new ArrayList<>();
        for ( Ord<RelDataTypeField> pair : Ord.zip( jdbcType.getFieldList() ) ) {
            final RelDataTypeField field = pair.e;
            final RelDataType type = field.getType();
            final RelDataType fieldType = x.isStruct() ? x.getFieldList().get( pair.i ).getType() : type;
            columns.add( metaData( typeFactory, columns.size(), field.getName(), type, fieldType, originList.get( pair.i ) ) );
        }
        return columns;
    }


    private ColumnMetaData metaData( JavaTypeFactory typeFactory, int ordinal, String fieldName, RelDataType type, RelDataType fieldType, List<String> origins ) {
        final ColumnMetaData.AvaticaType avaticaType = avaticaType( typeFactory, type, fieldType );
        return new ColumnMetaData(
                ordinal,
                false,
                true,
                false,
                false,
                type.isNullable()
                        ? DatabaseMetaData.columnNullable
                        : DatabaseMetaData.columnNoNulls,
                true,
                type.getPrecision(),
                fieldName,
                origin( origins, 0 ),
                origin( origins, 2 ),
                getPrecision( type ),
                getScale( type ),
                origin( origins, 1 ),
                null,
                avaticaType,
                true,
                false,
                false,
                avaticaType.columnClassName() );
    }


    private static String origin( List<String> origins, int offsetFromEnd ) {
        return origins == null || offsetFromEnd >= origins.size()
                ? null
                : origins.get( origins.size() - 1 - offsetFromEnd );
    }


    private static int getScale( RelDataType type ) {
        return type.getScale() == RelDataType.SCALE_NOT_SPECIFIED
                ? 0
                : type.getScale();
    }


    private static int getPrecision( RelDataType type ) {
        return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
                ? 0
                : type.getPrecision();
    }


    private ColumnMetaData.AvaticaType avaticaType( JavaTypeFactory typeFactory, RelDataType type, RelDataType fieldType ) {
        final String typeName = getTypeName( type );
        if ( type.getComponentType() != null ) {
            final ColumnMetaData.AvaticaType componentType = avaticaType( typeFactory, type.getComponentType(), null );
            final Type clazz = typeFactory.getJavaClass( type.getComponentType() );
            final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of( clazz );
            assert rep != null;
            return ColumnMetaData.array( componentType, typeName, rep );
        } else {
            int typeOrdinal = getTypeOrdinal( type );
            switch ( typeOrdinal ) {
                case Types.STRUCT:
                    final List<ColumnMetaData> columns = new ArrayList<>();
                    for ( RelDataTypeField field : type.getFieldList() ) {
                        columns.add( metaData( typeFactory, field.getIndex(), field.getName(), field.getType(), null, null ) );
                    }
                    return ColumnMetaData.struct( columns );
                case ExtraSqlTypes.GEOMETRY:
                    typeOrdinal = Types.VARCHAR;
                    // fall through
                default:
                    final Type clazz = typeFactory.getJavaClass( Util.first( fieldType, type ) );
                    final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of( clazz );
                    assert rep != null;
                    return ColumnMetaData.scalar( typeOrdinal, typeName, rep );
            }
        }
    }

    /**
     * Returns the type name in string form. Does not include precision, scale or whether nulls are allowed.
     * Example: "DECIMAL" not "DECIMAL(7, 2)"; "INTEGER" not "JavaType(int)".
     */
    private static String getTypeName( RelDataType type ) {
        final SqlTypeName sqlTypeName = type.getSqlTypeName();
        switch ( sqlTypeName ) {
            case ARRAY:
            case MULTISET:
            case MAP:
            case ROW:
                return type.toString(); // e.g. "INTEGER ARRAY"
            case INTERVAL_YEAR_MONTH:
                return "INTERVAL_YEAR_TO_MONTH";
            case INTERVAL_DAY_HOUR:
                return "INTERVAL_DAY_TO_HOUR";
            case INTERVAL_DAY_MINUTE:
                return "INTERVAL_DAY_TO_MINUTE";
            case INTERVAL_DAY_SECOND:
                return "INTERVAL_DAY_TO_SECOND";
            case INTERVAL_HOUR_MINUTE:
                return "INTERVAL_HOUR_TO_MINUTE";
            case INTERVAL_HOUR_SECOND:
                return "INTERVAL_HOUR_TO_SECOND";
            case INTERVAL_MINUTE_SECOND:
                return "INTERVAL_MINUTE_TO_SECOND";
            default:
                return sqlTypeName.getName(); // e.g. "DECIMAL", "INTERVAL_YEAR_MONTH"
        }
    }


    private int getTypeOrdinal( RelDataType type ) {
        return type.getSqlTypeName().getJdbcOrdinal();
    }


    /**
     * Implementation of Queryable.
     *
     * @param <T> element type
     */
    static class PolyphenyDbQueryable<T> extends BaseQueryable<T> {

        PolyphenyDbQueryable( QueryProvider queryProvider, Type elementType, Expression expression ) {
            super( queryProvider, elementType, expression );
        }

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
