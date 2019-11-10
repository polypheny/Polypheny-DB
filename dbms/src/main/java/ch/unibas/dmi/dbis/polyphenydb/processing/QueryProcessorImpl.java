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


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.QueryProcessor;
import ch.unibas.dmi.dbis.polyphenydb.statistic.StatisticsStore;
import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableCalc;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableInterpretable;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel;
import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.EnumerableRel.Prefer;
import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDefaultValue;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationGroup;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationManager;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationPage;
import ch.unibas.dmi.dbis.polyphenydb.information.InformationQueryPlan;
import ch.unibas.dmi.dbis.polyphenydb.interpreter.Interpreters;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.PolyphenyDbSignature;
import ch.unibas.dmi.dbis.polyphenydb.plan.ConventionTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable.ViewExpander;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptUtil;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitDef;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepPlanner;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgram;
import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepProgramBuilder;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbCatalogReader;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbSqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelRoot;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.AggregateReduceFunctionsRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.rules.CalcSplitRule;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexBuilder;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexProgram;
import ch.unibas.dmi.dbis.polyphenydb.runtime.ArrayBindable;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Bindable;
import ch.unibas.dmi.dbis.polyphenydb.runtime.PolyphenyDbException;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlBasicCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExecutableStatement;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainFormat;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExplainLevel;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlInsert;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlLiteral;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.PolyphenyDbSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParseException;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser.Config;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ExtraSqlTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.RelDecorrelator;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.SqlToRelConverter;
import ch.unibas.dmi.dbis.polyphenydb.sql2rel.StandardConvertletTable;
import ch.unibas.dmi.dbis.polyphenydb.tools.FrameworkConfig;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.tools.Planner;
import ch.unibas.dmi.dbis.polyphenydb.tools.Programs;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelConversionException;
import ch.unibas.dmi.dbis.polyphenydb.tools.ValidationException;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.SourceStringReader;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Type;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.AvaticaSeverity;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.CursorFactory;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.avatica.remote.AvaticaRuntimeException;
import org.apache.calcite.linq4j.Ord;
import org.apache.commons.lang3.time.StopWatch;


@Slf4j
public class QueryProcessorImpl implements QueryProcessor, ViewExpander {

    private static InformationPage informationPageLogical = new InformationPage( "informationPageLogicalQueryPlan", "Logical Query Plan" );
    private static InformationGroup informationGroupLogical = new InformationGroup( "informationGroupLogicalQueryPlan", informationPageLogical.getId() );
    private static InformationPage informationPagePhysical = new InformationPage( "informationPagePhysicalQueryPlan", "Physical Query Plan" );
    private static InformationGroup informationGroupPhysical = new InformationGroup( "informationGroupPhysicalQueryPlan", informationPagePhysical.getId() );

    private final Transaction transaction;
    private final StatisticsStore store = StatisticsStore.getInstance();

    QueryProcessorImpl( Transaction transaction ) {
        this.transaction = transaction;
    }


    @Override
    public PolyphenyDbSignature processSqlQuery( final String sql, final Config parserConfig ) {
        PolyphenyDbSchema rootSchema = transaction.getSchema();
        Context prepareContext = transaction.getPrepareContext();

        List<RelTraitDef> traitDefs = new ArrayList<>();
        traitDefs.add( ConventionTraitDef.INSTANCE );
        FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
                .parserConfig( parserConfig )
                .traitDefs( traitDefs )
                .defaultSchema( rootSchema.plus() )
                .prepareContext( prepareContext )
                //.sqlToRelConverterConfig( sqlToRelConfig )
                .programs( Programs.ofRules( Programs.RULE_SET ) )
                .build();
        Planner planner = Frameworks.getPlanner( frameworkConfig );

        //
        // Parsing
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolySQL statement ..." );
        }
        stopWatch.start();
        SqlNode parsed;
        try {
            parsed = planner.parse( sql );
        } catch ( SqlParseException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }/*
        try {
            parsed = parseSql( sql, frameworkConfig );
        } catch ( SqlParseException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }*/
        stopWatch.stop();
        if ( log.isTraceEnabled() ) {
            log.trace( "Parsed query: [{}]", parsed );
        }
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolySQL statement ... done. [{}]", stopWatch );
        }

        if ( parsed.isA( SqlKind.DDL ) ) {
            return prepareDdl( parsed );
        }

        // Add default values for unset fields
        if ( parsed.getKind() == SqlKind.INSERT ) {
            SqlInsert insert = (SqlInsert) parsed;



            SqlNodeList oldColumnList = insert.getTargetColumnList();
            if ( oldColumnList != null ) {
                CatalogCombinedTable combinedTable = getCatalogCombinedTable( prepareContext, transaction, (SqlIdentifier) insert.getTargetTable() );
                SqlNodeList newColumnList = new SqlNodeList( SqlParserPos.ZERO );
                SqlNode[][] newValues = new SqlNode[((SqlBasicCall) insert.getSource()).getOperands().length][combinedTable.getColumns().size()];
                int pos = 0;
                for ( CatalogColumn column : combinedTable.getColumns() ) {
                    // Add column
                    newColumnList.add( new SqlIdentifier( column.name, SqlParserPos.ZERO ) );

                    // Add value (loop because it can be a multi insert (insert into test(id) values (1),(2),(3))
                    int i = 0;
                    for ( SqlNode sqlNode : ((SqlBasicCall) insert.getSource()).getOperands() ) {
                        SqlBasicCall call = (SqlBasicCall) sqlNode;
                        int position = getPositionInSqlNodeList( oldColumnList, column.name );
                        if ( position >= 0 ) {
                            newValues[i][pos] = call.getOperands()[position];

                            if(column.type == PolySqlType.BIGINT){
                                System.out.println("insert in in db");

                                // TODO: move to confirmed insert
                                store.update(
                                        insert.getTargetTable().toString() + "." + column.name,
                                        column.name,
                                        // really hacky TODO: change later
                                        Integer.parseInt(newValues[i][pos].toString())
                                );


                            }

                        } else {
                            // Add value
                            if ( column.defaultValue != null ) {
                                CatalogDefaultValue defaultValue = column.defaultValue;
                                switch ( column.type ) {
                                    case BOOLEAN:
                                        newValues[i][pos] = SqlLiteral.createBoolean( Boolean.parseBoolean( column.defaultValue.value ), SqlParserPos.ZERO );
                                        break;
                                    case INTEGER:
                                    case DECIMAL:
                                    case BIGINT:
                                        newValues[i][pos] = SqlLiteral.createExactNumeric( column.defaultValue.value, SqlParserPos.ZERO );
                                        break;
                                    case REAL:
                                    case DOUBLE:
                                        newValues[i][pos] = SqlLiteral.createApproxNumeric( column.defaultValue.value, SqlParserPos.ZERO );
                                        break;
                                    case VARCHAR:
                                    case TEXT:
                                        newValues[i][pos] = SqlLiteral.createCharString( column.defaultValue.value, SqlParserPos.ZERO );
                                        break;
                                    default:
                                        throw new PolyphenyDbException( "Not yet supported default value type: " + defaultValue.type );
                                }
                            } else if ( column.nullable ) {
                                newValues[i][pos] = SqlLiteral.createNull( SqlParserPos.ZERO );
                            } else {
                                throw new PolyphenyDbException( "The not nullable field '" + column.name + "' is missing in the insert statement and has no default value defined." );
                            }
                        }
                        i++;
                    }
                    pos++;
                }
                // Add new column list
                insert.setColumnList( newColumnList );
                // Replace value in parser tree
                for ( int i = 0; i < newValues.length; i++ ) {
                    SqlBasicCall call = ((SqlBasicCall) ((SqlBasicCall) insert.getSource()).getOperands()[i]);
                    ((SqlBasicCall) insert.getSource()).getOperands()[i] = call.getOperator().createCall( call.getFunctionQuantifier(), call.getParserPosition(), newValues[i] );

                    store.store.forEach((k,v) -> {
                        System.out.println(k + " " + v.getId() + " " + v.getMin());
                        System.out.println(k + " " + v.getId() + " " + v.getMax());
                    });
                }
            }
        }

        //
        // Validation
        stopWatch.reset();
        if ( log.isDebugEnabled() ) {
            log.debug( "Validating SQL ..." );
        }
        stopWatch.start();
        /*final SqlConformance conformance = parserConfig.conformance();
        final PolyphenyDbCatalogReader catalogReader = createCatalogReader();
        PolyphenyDbSqlValidator validator = new PolyphenyDbSqlValidator( SqlStdOperatorTable.instance(), catalogReader, transaction.getTypeFactory(), conformance );
        Pair<SqlNode, RelDataType> validatePair;*/
        SqlNode validated;
        RelDataType type;
        try {
            Pair<SqlNode, RelDataType> validatePair = planner.validateAndGetType( parsed );
            validated = validatePair.left;
            type = validatePair.right;
        } catch ( ValidationException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }
        stopWatch.stop();
        if ( log.isTraceEnabled() ) {
            log.debug( "Validated query: [{}]", validated );
        }
        if ( log.isDebugEnabled() ) {
            log.debug( "Validating SELECT Statement ... done. [{}]", stopWatch );
        }

        //
        // Plan
        stopWatch.reset();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ..." );
        }


        stopWatch.start();
        RelNode logicalPlan;
        try {
            logicalPlan = planner.rel( validated ).rel;
        } catch ( RelConversionException e ) {
            throw new AvaticaRuntimeException( e.getLocalizedMessage(), -1, "", AvaticaSeverity.ERROR );
        }
        //RelNode logicalPlan = plan( validated, validator );
        if ( log.isTraceEnabled() ) {
            log.debug( "Logical query plan: [{}]", RelOptUtil.dumpPlan( "-- Logical Plan", logicalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ... done. [{}]", stopWatch );
        }

        if ( parsed.isA( SqlKind.DML ) ) {
            PolyphenyDbSignature processed = processQuery(logicalPlan, planner, parsed.getKind());
            if(parsed.getKind() == SqlKind.INSERT){
                System.out.println("is insert");
                System.out.println();
            }
            return processed;
        } else {
            return processQuery( logicalPlan );
        }
    }


    @Override
    public PolyphenyDbSignature processQuery( final RelNode logicalPlan ) {
        HepProgram hepProgram =
                new HepProgramBuilder()
                        .addRuleInstance( CalcSplitRule.INSTANCE )
                        .addRuleInstance( AggregateReduceFunctionsRule.INSTANCE )
                        /*.addRuleInstance( FilterTableScanRule.INSTANCE )
                        .addRuleInstance( FilterTableScanRule.INTERPRETER )
                        .addRuleInstance( ProjectTableScanRule.INSTANCE )
                        .addRuleInstance( ProjectTableScanRule.INTERPRETER ) */
                        .build();
        RelOptPlanner planner = new HepPlanner( hepProgram );
        return processQuery( logicalPlan, planner );
    }


    @Override
    public PolyphenyDbSignature processQuery( final RelNode logicalPlan, RelOptPlanner planner ) {
        if ( transaction.isAnalyze() ) {
            InformationManager queryAnalyzer = transaction.getQueryAnalyzer();
            queryAnalyzer.addPage( informationPageLogical );
            queryAnalyzer.addGroup( informationGroupLogical );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    "LogicalQueryPlan",
                    informationGroupLogical.getId(),
                    RelOptUtil.dumpPlan( "", logicalPlan, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        }

        final StopWatch stopWatch = new StopWatch();
        //
        // Optimization
        if ( log.isDebugEnabled() ) {
            log.debug( "Optimizing Statement  ..." );
        }
        stopWatch.start();
        planner.setRoot( logicalPlan );
        RelNode optimalPlan;
        optimalPlan = planner.findBestExp();
        if ( log.isTraceEnabled() ) {
            log.debug( "Physical query plan: [{}]", RelOptUtil.dumpPlan( "-- Physical Plan", optimalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        if ( transaction.isAnalyze() ) {
            InformationManager queryAnalyzer = transaction.getQueryAnalyzer();
            queryAnalyzer.addPage( informationPagePhysical );
            queryAnalyzer.addGroup( informationGroupPhysical );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    "PhysicalQueryPlan",
                    informationGroupPhysical.getId(),
                    RelOptUtil.dumpPlan( "", optimalPlan, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        }
        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Optimizing Statement ... done. [{}]", stopWatch );
        }

        //
        // Prepare to be executed
        PolyphenyDbSignature signature = prepare( optimalPlan, optimalPlan.getRowType() );


       /* PolyphenyDbSignature signature = null;
        if ( parsed.getKind() == SqlKind.EXPLAIN ) {
           /* if ( ((SqlExplain) parsed).getExplicandum().isA( SqlKind.QUERY ) ) {
                signature = DmlExecutionEngine.getInstance().explain( h, statement, planner, stopWatch, (SqlExplain) parsed );
            } else {
                throw new RuntimeException( "EXPLAIN is currently only supported for SELECT queries!" );
            }
        } else if ( parsed.isA( SqlKind.QUERY ) ) {
            signature = prepare( optimalPlan, type );
        } else if ( parsed.isA( SqlKind.DML ) ) {
            signature = prepare( optimalPlan, type );
        } else {
            throw new RuntimeException( "Unknown or unsupported query type: " + parsed.getKind().name() );
        }
        */
        return signature;
    }


    @Override
    public PolyphenyDbSignature processQuery( final RelNode logicalPlan, Planner planner, SqlKind kind ) {
        InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                "LogicalQueryPlan",
                informationGroupLogical.getId(),
                RelOptUtil.dumpPlan( "", logicalPlan, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
        InformationManager.getInstance().registerInformation( informationQueryPlan );

        final StopWatch stopWatch = new StopWatch();
        //
        // Optimization
        if ( log.isDebugEnabled() ) {
            log.debug( "Optimizing Statement  ..." );
        }
        stopWatch.start();
        RelNode optimalPlan;
        try {
            optimalPlan = planner.transform( 0, planner.getEmptyTraitSet().replace( EnumerableConvention.INSTANCE ), logicalPlan );
        } catch ( RelConversionException e ) {
            throw new RuntimeException( e );
        }
        if ( log.isTraceEnabled() ) {
            log.trace( "Physical query plan: [{}]", RelOptUtil.dumpPlan( "-- Physical Plan", optimalPlan, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        informationQueryPlan = new InformationQueryPlan(
                "PhysicalQueryPlan",
                informationGroupPhysical.getId(),
                RelOptUtil.dumpPlan( "", optimalPlan, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
        InformationManager.getInstance().registerInformation( informationQueryPlan );
        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Optimizing Statement ... done. [{}]", stopWatch );
        }

        //
        // Prepare to be executed
        RelRoot root = RelRoot.of( optimalPlan, kind );
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

        return new PolyphenyDbSignature(
                "",
                ImmutableList.of(),
                ImmutableMap.of(),
                null,
                null,
                CursorFactory.ARRAY,
                transaction.getSchema(),
                ImmutableList.of(),
                -1,
                bindable,
                StatementType.IS_DML );
    }


    private PolyphenyDbSignature prepare( final RelNode optimalPlan, final RelDataType type ) {
        ArrayBindable bindable = Interpreters.bindable( optimalPlan );
        final CursorFactory cf = CursorFactory.ARRAY;
        final JavaTypeFactory typeFactory = transaction.getTypeFactory();
        final List<List<String>> origins = Collections.nCopies( optimalPlan.getRowType().getFieldCount(), null );
        List<ColumnMetaData> columnMetaData = getColumnMetaDataList( typeFactory, optimalPlan.getRowType(), optimalPlan.getRowType(), origins );

        log.info("test");
        log.info(Arrays.toString(columnMetaData.toArray()));
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
            jdbcType = transaction.getTypeFactory().builder().add( "$0", type ).build();
        }

        return new PolyphenyDbSignature(
                "",
                parameters,
                ImmutableMap.of(),
                jdbcType,
                columnMetaData,
                cf,
                transaction.getSchema(),
                ImmutableList.of(),
                -1,
                bindable,
                null );
    }


    private SqlNode parseSql( String sql, FrameworkConfig config ) throws SqlParseException {
        SqlParser parser = SqlParser.create( new SourceStringReader( sql ), config.getParserConfig() );
        return parser.parseStmt();
    }


    private PolyphenyDbSignature prepareDdl( SqlNode parsed ) {
        if ( parsed instanceof SqlExecutableStatement ) {
            ((SqlExecutableStatement) parsed).execute( transaction.getPrepareContext(), transaction );
            return new PolyphenyDbSignature<>(
                    parsed.toSqlString( PolyphenyDbSqlDialect.DEFAULT ).getSql(),
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    null,
                    ImmutableList.of(),
                    Meta.CursorFactory.OBJECT,
                    transaction.getSchema(),
                    ImmutableList.of(),
                    -1,
                    null,
                    Meta.StatementType.OTHER_DDL );
        } else {
            throw new RuntimeException( "All DDL queries should be of a type that inherits SqlExecutableStatement. But this one is of type " + parsed.getClass() );
        }
    }


    private Pair<SqlNode, RelDataType> validateSql( PolyphenyDbSqlValidator validator, final SqlNode parsed ) throws ValidationException {
        validator.setIdentifierExpansion( true );
        SqlNode validated;
        try {
            validated = validator.validate( parsed );
        } catch ( RuntimeException e ) {
            throw new ValidationException( e );
        }

        return Pair.of( validated, validator.getValidatedNodeType( validated ) );
    }


    private PolyphenyDbCatalogReader createCatalogReader() {
        return new PolyphenyDbCatalogReader(
                PolyphenyDbSchema.from( transaction.getSchema().plus() ),
                PolyphenyDbSchema.from( transaction.getSchema().plus() ).path( null ),
                transaction.getTypeFactory() );
    }


    private void authorize( final SqlNode validated ) {
        // TODO
    }


    private RelNode plan( RelOptPlanner planner, final SqlNode validated, PolyphenyDbSqlValidator validator ) {
        SqlToRelConverter.ConfigBuilder sqlToRelConfigBuilder = SqlToRelConverter.configBuilder();
        SqlToRelConverter.Config sqlToRelConfig = sqlToRelConfigBuilder.build();
        final RexBuilder rexBuilder = new RexBuilder( transaction.getTypeFactory() );
        final RelOptCluster cluster = RelOptCluster.create( planner, rexBuilder );
        final SqlToRelConverter.Config config =
                SqlToRelConverter.configBuilder()
                        .withConfig( sqlToRelConfig )
                        .withTrimUnusedFields( false )
                        .withConvertTableAccess( false )
                        .build();
        final SqlToRelConverter sqlToRelConverter = new SqlToRelConverter( this, validator, createCatalogReader(), cluster, StandardConvertletTable.INSTANCE, config );
        RelRoot logicalPlan = sqlToRelConverter.convertQuery( validated, false, true );
        logicalPlan = logicalPlan.withRel( sqlToRelConverter.flattenTypes( logicalPlan.rel, true ) );
        final RelBuilder relBuilder = config.getRelBuilderFactory().create( cluster, null );
        logicalPlan = logicalPlan.withRel( RelDecorrelator.decorrelateQuery( logicalPlan.rel, relBuilder ) );
        return logicalPlan.rel;
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


    /**
     * Returns a relational expression that is to be substituted for an access to a SQL view.
     *
     * @param rowType Row type of the view
     * @param queryString Body of the view
     * @param schemaPath Path of a schema wherein to find referenced tables
     * @param viewPath Path of the view, ending with its name; may be null
     * @return Relational expression
     */
    @Override
    public RelRoot expandView( RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath ) {
        return null; // TODO: Implement
    }


    private CatalogTable getCatalogTable( Context context, Transaction transaction, SqlIdentifier tableName ) {
        CatalogTable catalogTable;
        try {
            long schemaId;
            String tableOldName;
            if ( tableName.names.size() == 3 ) { // DatabaseName.SchemaName.TableName
                schemaId = transaction.getCatalog().getSchema( tableName.names.get( 0 ), tableName.names.get( 1 ) ).id;
                tableOldName = tableName.names.get( 2 );
            } else if ( tableName.names.size() == 2 ) { // SchemaName.TableName
                schemaId = transaction.getCatalog().getSchema( context.getDatabaseId(), tableName.names.get( 0 ) ).id;
                tableOldName = tableName.names.get( 1 );
            } else { // TableName
                schemaId = transaction.getCatalog().getSchema( context.getDatabaseId(), context.getDefaultSchemaName() ).id;
                tableOldName = tableName.names.get( 0 );
            }
            catalogTable = transaction.getCatalog().getTable( schemaId, tableOldName );
        } catch ( UnknownDatabaseException e ) {
            throw SqlUtil.newContextException( tableName.getParserPosition(), RESOURCE.databaseNotFound( tableName.toString() ) );
        } catch ( UnknownSchemaException e ) {
            throw SqlUtil.newContextException( tableName.getParserPosition(), RESOURCE.schemaNotFound( tableName.toString() ) );
        } catch ( UnknownTableException e ) {
            throw SqlUtil.newContextException( tableName.getParserPosition(), RESOURCE.tableNotFound( tableName.toString() ) );
        } catch ( UnknownCollationException | UnknownSchemaTypeException | GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
        return catalogTable;
    }


    private CatalogCombinedTable getCatalogCombinedTable( Context context, Transaction transaction, SqlIdentifier tableName ) {
        try {
            return transaction.getCatalog().getCombinedTable( getCatalogTable( context, transaction, tableName ).id );
        } catch ( GenericCatalogException | UnknownTableException e ) {
            throw new RuntimeException( e );
        }
    }


    private int getPositionInSqlNodeList( SqlNodeList columnList, String name ) {
        int i = 0;
        for ( SqlNode node : columnList.getList() ) {
            SqlIdentifier identifier = (SqlIdentifier) node;
            if ( RuntimeConfig.CASE_SENSITIVE.getBoolean() ) {
                if ( identifier.getSimple().equals( name ) ) {
                    return i;
                }
            } else {
                if ( identifier.getSimple().equalsIgnoreCase( name ) ) {
                    return i;
                }
            }
            i++;
        }
        return -1;
    }
}
