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

package ch.unibas.dmi.dbis.polyphenydb.processing;


import ch.unibas.dmi.dbis.polyphenydb.QueryProcessor;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableCalc;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableInterpretable;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel.Prefer;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGroup;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationQueryPlan;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.BindableConvention;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Interpreters;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.PolyphenyDbSignature;
import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare.CatalogReader;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare.PreparedResult;
import ch.unibas.dmi.dbis.polyphenydb.prepare.Prepare.PreparedResultImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollations;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelRoot;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Bindable;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Typed;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainFormat;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ExtraSqlTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformance;
import ch.unibas.dmi.dbis.polyphenydb.tools.Program;
import ch.unibas.dmi.dbis.polyphenydb.tools.Programs;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableIntList;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Type;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.linq4j.Ord;
import org.apache.commons.lang3.time.StopWatch;


@Slf4j
public abstract class AbstractQueryProcessor implements QueryProcessor {

    private final Transaction transaction;
    private final RexBuilder rexBuilder;

    protected static final boolean ENABLE_BINDABLE = true;
    protected static final boolean ENABLE_COLLATION_TRAIT = true;
    protected static final boolean ENABLE_ENUMERABLE = true;
    protected static final boolean CONSTANT_REDUCTION = false;


    protected AbstractQueryProcessor( Transaction transaction ) {
        this.transaction = transaction;
        this.rexBuilder = new RexBuilder( transaction.getTypeFactory() );
    }


    @Override
    public PolyphenyDbSignature prepareQuery( RelRoot logicalRoot ) {

        final StopWatch stopWatch = new StopWatch();

        //
        // Optimization
        if ( log.isDebugEnabled() ) {
            log.debug( "Preparing statement  ..." );
        }
        stopWatch.start();

        // Structured type flattening, view expansion, and plugging in physical storage.
        //logicalRoot = logicalRoot.withRel( flattenTypes( logicalRoot.rel, true ) );

        RelRoot optimalRoot = optimize( logicalRoot );

        final RelDataType jdbcType = makeStruct( rexBuilder.getTypeFactory(), logicalRoot.validatedRowType );

        // For transformation from DML -> DML, use result of rewrite (e.g. UPDATE -> MERGE).  For anything else (e.g. CALL -> SELECT), use original kind.
        //if ( !optimalRoot.kind.belongsTo( SqlKind.DML ) ) {
        //    optimalRoot = optimalRoot.withKind( sqlNodeOriginal.getKind() );
        //}

        PolyphenyDbSignature signature = implement( optimalRoot, jdbcType );

        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Preparing statement ... done. [{}]", stopWatch );
        }

        return signature;
    }


    private PolyphenyDbSignature implement( RelRoot optimalRoot, RelDataType jdbcType ) {
        List<List<String>> fieldOrigins = Collections.nCopies( jdbcType.getFieldCount(), null );

        if ( log.isTraceEnabled() ) {
            log.debug( "Physical query plan: [{}]", RelOptUtil.dumpPlan( "-- Physical Plan", optimalRoot.rel, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        if ( transaction.isAnalyze() ) {
            InformationManager queryAnalyzer = transaction.getQueryAnalyzer();
            InformationPage page = new InformationPage( "informationPagePhysicalQueryPlan", "Physical Query Plan" );
            InformationGroup group = new InformationGroup( page, "Physical Query Plan" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    group,
                    RelOptUtil.dumpPlan( "Physical Query Plan", optimalRoot.rel, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        }

        PreparedResult preparedResult = implement( optimalRoot, fieldOrigins );

        final List<AvaticaParameter> parameters = new ArrayList<>();
        for ( RelDataTypeField field : preparedResult.getParameterRowType().getFieldList() ) {
            RelDataType type = field.getType();
            parameters.add(
                    new AvaticaParameter(
                            false,
                            getPrecision( type ),
                            getScale( type ),
                            getTypeOrdinal( type ),
                            getTypeName( type ),
                            getClassName( type ),
                            field.getName() ) );
        }

        final RelDataType x;
        switch ( optimalRoot.kind ) {
            case INSERT:
            case DELETE:
            case UPDATE:
            case EXPLAIN:
                // FIXME: getValidatedNodeType is wrong for DML
                x = RelOptUtil.createDmlRowType( optimalRoot.kind, transaction.getTypeFactory() );
                break;
            default:
                x = optimalRoot.validatedRowType;
        }
        final List<ColumnMetaData> columns = getColumnMetaDataList( transaction.getTypeFactory(), x, makeStruct( transaction.getTypeFactory(), x ), preparedResult.getFieldOrigins() );
        Class resultClazz = null;
        if ( preparedResult instanceof Typed ) {
            resultClazz = (Class) ((Typed) preparedResult).getElementType();
        }
        final CursorFactory cursorFactory =
                optimalRoot.rel.getConvention() == BindableConvention.INSTANCE
                        ? CursorFactory.ARRAY
                        : CursorFactory.deduce( columns, resultClazz );
        //noinspection unchecked
        final Bindable bindable = preparedResult.getBindable( cursorFactory );

        return new PolyphenyDbSignature<Object[]>(
                "",
                parameters,
                ImmutableMap.of(),
                jdbcType,
                columns,
                cursorFactory,
                transaction.getSchema(),
                ImmutableList.of(),
                -1,
                bindable,
                getStatementType( preparedResult ) );
    }


    private RelRoot optimize( RelRoot logicalRoot ) {
        RelNode logicalPlan = logicalRoot.rel;
        final RelDataType rowType = logicalPlan.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final RelCollation collation =
                logicalPlan instanceof Sort
                        ? ((Sort) logicalPlan).collation
                        : RelCollations.EMPTY;
        RelRoot root = new RelRoot( logicalPlan, logicalPlan.getRowType(), logicalRoot.kind, fields, collation );

        final RelTraitSet desiredTraits = root.rel.getTraitSet()
                .replace( BindableConvention.INSTANCE )
                .replace( root.collation )
                .simplify();

        final Program program = Programs.standard();
        final RelNode rootRel4 = program.run( getPlanner(), root.rel, desiredTraits );

        return root.withRel( rootRel4 );
    }


    private PreparedResult implement( RelRoot root, List<List<String>> fieldOrigins ) {
        final Prefer prefer = Prefer.ARRAY;
        final Convention resultConvention =
                ENABLE_BINDABLE
                        ? BindableConvention.INSTANCE
                        : EnumerableConvention.INSTANCE;

        final Bindable bindable;
        if ( resultConvention == BindableConvention.INSTANCE ) {
            bindable = Interpreters.bindable( root.rel );
        } else {
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

            try {
                CatalogReader.THREAD_LOCAL.set( transaction.getCatalogReader() );
                final SqlConformance conformance = transaction.getPrepareContext().config().conformance();

                final Map<String, Object> internalParameters = new LinkedHashMap<>();
                internalParameters.put( "_conformance", conformance );

                bindable = EnumerableInterpretable.toBindable( internalParameters, transaction.getPrepareContext().spark(), enumerable, prefer, null );
            } finally {
                CatalogReader.THREAD_LOCAL.remove();
            }
        }

        RelDataType resultType = root.rel.getRowType();
        boolean isDml = root.kind.belongsTo( SqlKind.DML );

        return new PreparedResultImpl(
                resultType,
                rexBuilder.getTypeFactory().builder().build(),
                fieldOrigins,
                root.collation.getFieldCollations().isEmpty()
                        ? ImmutableList.of()
                        : ImmutableList.of( root.collation ),
                root.rel,
                mapTableModOp( isDml, root.kind ),
                isDml ) {
            @Override
            public String getCode() {
                throw new UnsupportedOperationException();
            }


            @Override
            public Bindable getBindable( CursorFactory cursorFactory ) {
                return bindable;
            }


            @Override
            public Type getElementType() {
                return ((Typed) bindable).getElementType();
            }
        };
    }


    private StatementType getStatementType( PreparedResult preparedResult ) {
        if ( preparedResult.isDml() ) {
            return StatementType.IS_DML;
        } else {
            return StatementType.SELECT;
        }
    }


    private static RelDataType makeStruct( RelDataTypeFactory typeFactory, RelDataType type ) {
        if ( type.isStruct() ) {
            return type;
        }
        return typeFactory.builder().add( "$0", type ).build();
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


    private static String getClassName( RelDataType type ) {
        return Object.class.getName();
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


    protected LogicalTableModify.Operation mapTableModOp( boolean isDml, SqlKind sqlKind ) {
        if ( !isDml ) {
            return null;
        }
        switch ( sqlKind ) {
            case INSERT:
                return LogicalTableModify.Operation.INSERT;
            case DELETE:
                return LogicalTableModify.Operation.DELETE;
            case MERGE:
                return LogicalTableModify.Operation.MERGE;
            case UPDATE:
                return LogicalTableModify.Operation.UPDATE;
            default:
                return null;
        }
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

}
