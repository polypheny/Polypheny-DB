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

package org.polypheny.db.language;

import java.io.Reader;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import org.apache.calcite.avatica.util.TimeUnit;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.ChainedOperatorTable;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.cypher.CypherRegisterer;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.NodeToAlgConverter;
import org.polypheny.db.languages.NodeToAlgConverter.Config;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.languages.ParserFactory;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.RexConvertletTable;
import org.polypheny.db.languages.UnsupportedLanguageOperation;
import org.polypheny.db.languages.core.MqlRegisterer;
import org.polypheny.db.languages.sql.parser.impl.SqlParserImpl;
import org.polypheny.db.mql.parser.impl.MqlParserImpl;
import org.polypheny.db.nodes.DataTypeSpec;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.schema.AggregateFunction;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.TableFunction;
import org.polypheny.db.schema.TableMacro;
import org.polypheny.db.sql.SqlRegisterer;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlIntervalQualifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.dialect.AnsiSqlDialect;
import org.polypheny.db.sql.language.fun.OracleSqlOperatorTable;
import org.polypheny.db.sql.language.fun.SqlBitOpAggFunction;
import org.polypheny.db.sql.language.fun.SqlMinMaxAggFunction;
import org.polypheny.db.sql.language.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.language.fun.SqlSumAggFunction;
import org.polypheny.db.sql.language.fun.SqlSumEmptyIsZeroAggFunction;
import org.polypheny.db.sql.language.parser.SqlAbstractParserImpl;
import org.polypheny.db.sql.language.parser.SqlParser;
import org.polypheny.db.sql.language.pretty.SqlPrettyWriter;
import org.polypheny.db.sql.language.util.SqlString;
import org.polypheny.db.sql.language.validate.PolyphenyDbSqlValidator;
import org.polypheny.db.sql.language.validate.SqlUserDefinedAggFunction;
import org.polypheny.db.sql.language.validate.SqlUserDefinedFunction;
import org.polypheny.db.sql.language.validate.SqlUserDefinedTableFunction;
import org.polypheny.db.sql.language.validate.SqlUserDefinedTableMacro;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.sql2alg.SqlRexConvertletTable;
import org.polypheny.db.sql.sql2alg.SqlToAlgConverter;
import org.polypheny.db.sql.sql2alg.StandardConvertletTable;
import org.polypheny.db.type.PolyIntervalQualifier;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.FamilyOperandTypeChecker;
import org.polypheny.db.type.checker.PolySingleOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.Optionality;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;
import org.polypheny.db.util.Util;
import org.slf4j.Logger;


public class LanguageManagerImpl extends LanguageManager {

    static {
        if ( !SqlRegisterer.isInit() ) {
            SqlRegisterer.registerOperators();
        }
        if ( !MqlRegisterer.isInit() ) {
            MqlRegisterer.registerOperators();
        }
        if ( !CypherRegisterer.isInit() ) {
            CypherRegisterer.registerOperators();
        }
    }


    @Override
    public Validator createValidator( QueryLanguage language, Context context, PolyphenyDbCatalogReader catalogReader ) {
        if ( language == QueryLanguage.SQL ) {
            return getSqlValidator( context, catalogReader );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    private PolyphenyDbSqlValidator getSqlValidator( Context context, PolyphenyDbCatalogReader catalogReader ) {
        final OperatorTable opTab0 = context.config().fun( OperatorTable.class, getInstance().getStdOperatorTable() );
        final OperatorTable opTab = ChainedOperatorTable.of( opTab0, catalogReader );
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        final Conformance conformance = context.config().conformance();
        return new PolyphenyDbSqlValidator( opTab, catalogReader, typeFactory, conformance );
    }


    @Override
    public NodeToAlgConverter createToRelConverter(
            QueryLanguage language,
            Validator validator,
            CatalogReader catalogReader,
            AlgOptCluster cluster,
            RexConvertletTable convertletTable,
            Config config ) {
        if ( language == QueryLanguage.SQL ) {
            return getSqlToRelConverter( (SqlValidator) validator, catalogReader, cluster, (SqlRexConvertletTable) convertletTable, config );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    private SqlToAlgConverter getSqlToRelConverter(
            SqlValidator validator,
            CatalogReader catalogReader,
            AlgOptCluster cluster,
            SqlRexConvertletTable convertletTable,
            Config config ) {
        return new SqlToAlgConverter( validator, catalogReader, cluster, convertletTable, config );
    }


    @Override
    public RexConvertletTable getStandardConvertlet() {
        return StandardConvertletTable.INSTANCE;
    }


    @Override
    public OperatorTable getStdOperatorTable() {
        return SqlStdOperatorTable.instance();
    }


    @Override
    public Validator createPolyphenyValidator(
            QueryLanguage language,
            OperatorTable operatorTable,
            PolyphenyDbCatalogReader catalogReader,
            JavaTypeFactory typeFactory,
            Conformance conformance ) {
        if ( language == QueryLanguage.SQL ) {
            return new PolyphenyDbSqlValidator( operatorTable, catalogReader, typeFactory, conformance );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public ParserFactory getFactory( QueryLanguage language ) {
        if ( language == QueryLanguage.SQL ) {
            return SqlParserImpl.FACTORY;
        } else if ( language == QueryLanguage.MONGO_QL ) {
            return MqlParserImpl.FACTORY;
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public Parser getParser( QueryLanguage language, Reader reader, ParserConfig parserConfig ) {
        if ( language == QueryLanguage.SQL ) {
            SqlAbstractParserImpl parser = (SqlAbstractParserImpl) parserConfig.parserFactory().getParser( reader );
            return new SqlParser( parser, parserConfig );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public OperatorTable getOracleOperatorTable() {
        return OracleSqlOperatorTable.instance();
    }


    @Override
    public Logger getLogger( QueryLanguage language, Class<AlgNode> algNodeClass ) {
        if ( language == QueryLanguage.SQL ) {
            return SqlToAlgConverter.SQL2REL_LOGGER;
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public Identifier createIdentifier( QueryLanguage language, String name, ParserPos zero ) {
        if ( language == QueryLanguage.SQL ) {
            return new SqlIdentifier( name, zero );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public DataTypeSpec createDataTypeSpec(
            QueryLanguage language,
            Identifier typeIdentifier,
            int precision,
            int scale,
            String charSetName,
            TimeZone o,
            ParserPos pos ) {
        if ( language == QueryLanguage.SQL ) {
            return new SqlDataTypeSpec(
                    (SqlIdentifier) typeIdentifier,
                    precision,
                    scale,
                    charSetName,
                    o,
                    pos );
        }
        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public DataTypeSpec createDataTypeSpec(
            QueryLanguage language,
            Identifier typeIdentifier,
            Identifier componentTypeIdentifier,
            int precision,
            int scale,
            int dimension,
            int cardinality,
            String charSetName,
            TimeZone o,
            boolean nullable,
            ParserPos zero ) {
        if ( language == QueryLanguage.SQL ) {
            return new SqlDataTypeSpec(
                    (SqlIdentifier) typeIdentifier,
                    (SqlIdentifier) componentTypeIdentifier,
                    precision,
                    scale,
                    dimension,
                    cardinality,
                    charSetName,
                    o,
                    nullable,
                    zero );
        }
        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public IntervalQualifier createIntervalQualifier(
            QueryLanguage language,
            TimeUnit startUnit,
            int startPrecision,
            TimeUnit endUnit,
            int fractionalSecondPrecision,
            ParserPos zero ) {
        if ( language == QueryLanguage.SQL ) {
            return new SqlIntervalQualifier( startUnit, startPrecision, endUnit, fractionalSecondPrecision, zero );
        }
        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public Literal createLiteral( QueryLanguage language, PolyType polyType, Object o, ParserPos pos ) {
        if ( language == QueryLanguage.SQL ) {
            switch ( polyType ) {
                case BOOLEAN:
                    return SqlLiteral.createBoolean( (Boolean) o, pos );
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case DECIMAL:
                    return SqlLiteral.createExactNumeric( o.toString(), pos );
                case JSON:
                case VARCHAR:
                case CHAR:
                    return SqlLiteral.createCharString( (String) o, pos );
                case VARBINARY:
                case BINARY:
                    return SqlLiteral.createBinaryString( (byte[]) o, pos );
                case DATE:
                    return SqlLiteral.createDate(
                            o instanceof Calendar
                                    ? DateString.fromCalendarFields( (Calendar) o )
                                    : (DateString) o,
                            pos );
                case TIME:
                    return SqlLiteral.createTime(
                            o instanceof Calendar
                                    ? TimeString.fromCalendarFields( (Calendar) o )
                                    : (TimeString) o,
                            0 /* todo */,
                            pos );
                case TIMESTAMP:
                    return SqlLiteral.createTimestamp(
                            o instanceof Calendar
                                    ? TimestampString.fromCalendarFields( (Calendar) o )
                                    : (TimestampString) o,
                            0 /* todo */,
                            pos );
                default:
                    throw Util.unexpected( polyType );
            }
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public AggFunction createMinMaxAggFunction( QueryLanguage language, Kind kind ) {
        if ( language == QueryLanguage.SQL ) {
            return new SqlMinMaxAggFunction( kind );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public AggFunction createSumEmptyIsZeroFunction( QueryLanguage language ) {
        if ( language == QueryLanguage.SQL ) {
            return new SqlSumEmptyIsZeroAggFunction();
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public AggFunction createBitOpAggFunction( QueryLanguage language, Kind kind ) {
        if ( language == QueryLanguage.SQL ) {
            return new SqlBitOpAggFunction( kind );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public AggFunction createSumAggFunction( QueryLanguage language, AlgDataType type ) {
        if ( language == QueryLanguage.SQL ) {
            return new SqlSumAggFunction( type );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public Operator createFunction(
            QueryLanguage language,
            String name,
            Kind kind,
            PolyReturnTypeInference returnTypeInference,
            PolyOperandTypeInference o,
            PolySingleOperandTypeChecker typeChecker,
            FunctionCategory system ) {
        if ( language == QueryLanguage.SQL ) {
            return new SqlFunction(
                    name,
                    kind,
                    returnTypeInference, // returns boolean since we'll AND it
                    o,
                    typeChecker, // takes a numeric param
                    system );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public void createIntervalTypeString( StringBuilder sb, PolyIntervalQualifier intervalQualifier ) {
        sb.append( "INTERVAL " );
        final SqlDialect dialect = AnsiSqlDialect.DEFAULT;
        final SqlPrettyWriter writer = new SqlPrettyWriter( dialect );
        writer.setAlwaysUseParentheses( false );
        writer.setSelectListItemsOnSeparateLines( false );
        writer.setIndentation( 0 );
        new SqlIntervalQualifier( intervalQualifier ).unparse( writer, 0, 0 );
        final String sql = writer.toString();
        sb.append( new SqlString( dialect, sql ).getSql() );
    }


    @Override
    public Operator createUserDefinedFunction(
            QueryLanguage language,
            Identifier name,
            PolyReturnTypeInference infer,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            List<AlgDataType> paramTypes,
            Function function ) {
        if ( language == QueryLanguage.SQL ) {
            return new SqlUserDefinedFunction( (SqlIdentifier) name, infer, explicit, typeChecker, paramTypes, function );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public Operator createUserDefinedAggFunction(
            QueryLanguage language,
            Identifier name,
            PolyReturnTypeInference infer,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            AggregateFunction function,
            boolean requiresOrder,
            boolean requiresOver,
            Optionality optionality,
            AlgDataTypeFactory typeFactory ) {
        if ( language == QueryLanguage.SQL ) {
            return new SqlUserDefinedAggFunction(
                    (SqlIdentifier) name,
                    infer,
                    explicit,
                    typeChecker,
                    function,
                    requiresOrder,
                    requiresOver,
                    optionality,
                    typeFactory );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public Operator createUserDefinedTableMacro(
            QueryLanguage language,
            Identifier name,
            PolyReturnTypeInference typeInference,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            List<AlgDataType> paramTypes,
            TableMacro function ) {
        if ( language == QueryLanguage.SQL ) {
            return new SqlUserDefinedTableMacro( (SqlIdentifier) name, typeInference, explicit, typeChecker, paramTypes, function );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public Operator createUserDefinedTableFunction(
            QueryLanguage language,
            Identifier name,
            PolyReturnTypeInference typeInference,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            List<AlgDataType> paramTypes,
            TableFunction function ) {
        if ( language == QueryLanguage.SQL ) {
            return new SqlUserDefinedTableFunction( (SqlIdentifier) name, typeInference, explicit, typeChecker, paramTypes, function );
        }

        throw new UnsupportedLanguageOperation( language );
    }


}
