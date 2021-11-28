/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.languages.core;

import java.io.Reader;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import org.apache.calcite.avatica.util.TimeUnit;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.core.enums.FunctionCategory;
import org.polypheny.db.core.enums.Kind;
import org.polypheny.db.core.fun.AggFunction;
import org.polypheny.db.core.nodes.DataTypeSpec;
import org.polypheny.db.core.nodes.Identifier;
import org.polypheny.db.core.nodes.IntervalQualifier;
import org.polypheny.db.core.nodes.Literal;
import org.polypheny.db.core.nodes.Operator;
import org.polypheny.db.core.operators.ChainedOperatorTable;
import org.polypheny.db.core.operators.OperatorTable;
import org.polypheny.db.core.util.Conformance;
import org.polypheny.db.core.validate.Validator;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.NodeToRelConverter;
import org.polypheny.db.languages.NodeToRelConverter.Config;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.languages.ParserFactory;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.RexConvertletTable;
import org.polypheny.db.languages.sql.SqlDataTypeSpec;
import org.polypheny.db.languages.sql.SqlDialect;
import org.polypheny.db.languages.sql.SqlFunction;
import org.polypheny.db.languages.sql.SqlIdentifier;
import org.polypheny.db.languages.sql.SqlIntervalQualifier;
import org.polypheny.db.languages.sql.SqlLiteral;
import org.polypheny.db.languages.sql.dialect.AnsiSqlDialect;
import org.polypheny.db.languages.sql.fun.OracleSqlOperatorTable;
import org.polypheny.db.languages.sql.fun.SqlBitOpAggFunction;
import org.polypheny.db.languages.sql.fun.SqlMinMaxAggFunction;
import org.polypheny.db.languages.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.languages.sql.fun.SqlSumAggFunction;
import org.polypheny.db.languages.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.polypheny.db.languages.sql.parser.SqlAbstractParserImpl;
import org.polypheny.db.languages.sql.parser.SqlParser;
import org.polypheny.db.languages.sql.parser.impl.SqlParserImpl;
import org.polypheny.db.languages.sql.pretty.SqlPrettyWriter;
import org.polypheny.db.languages.sql.util.SqlString;
import org.polypheny.db.languages.sql.validate.PolyphenyDbSqlValidator;
import org.polypheny.db.languages.sql.validate.SqlUserDefinedAggFunction;
import org.polypheny.db.languages.sql.validate.SqlUserDefinedFunction;
import org.polypheny.db.languages.sql.validate.SqlUserDefinedTableFunction;
import org.polypheny.db.languages.sql.validate.SqlUserDefinedTableMacro;
import org.polypheny.db.languages.sql.validate.SqlValidator;
import org.polypheny.db.languages.sql2rel.SqlRexConvertletTable;
import org.polypheny.db.languages.sql2rel.SqlToRelConverter;
import org.polypheny.db.languages.sql2rel.StandardConvertletTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable.ViewExpander;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.schema.AggregateFunction;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.TableFunction;
import org.polypheny.db.schema.TableMacro;
import org.polypheny.db.type.PolyIntervalQualifier;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.FamilyOperandTypeChecker;
import org.polypheny.db.type.checker.PolySingleOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.Optionality;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;
import org.polypheny.db.util.Util;
import org.slf4j.Logger;

public class LanguageManagerImpl extends LanguageManager {

    static {
        if( !SqlRegisterer.isInit() ){
            SqlRegisterer.registerOperators();
        }
        if( !MqlRegisterer.isInit()){
            MqlRegisterer.registerOperators();
        }
    }


    @Override
    public Validator createValidator( QueryLanguage language, Context context, PolyphenyDbCatalogReader catalogReader ) {
        final OperatorTable opTab0 = context.config().fun( OperatorTable.class, getInstance().getStdOperatorTable() );
        final OperatorTable opTab = ChainedOperatorTable.of( opTab0, catalogReader );
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        final Conformance conformance = context.config().conformance();
        return new PolyphenyDbSqlValidator( opTab, catalogReader, typeFactory, conformance );
    }


    @Override
    public NodeToRelConverter createToRelConverter( QueryLanguage sql,
            ViewExpander polyphenyDbPreparingStmt,
            Validator validator,
            CatalogReader catalogReader,
            RelOptCluster cluster,
            RexConvertletTable convertletTable,
            Config config ) {
        return new SqlToRelConverter( polyphenyDbPreparingStmt, (SqlValidator) validator, catalogReader, cluster, (SqlRexConvertletTable) convertletTable, config );
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
    public Validator createPolyphenyValidator( QueryLanguage language, OperatorTable operatorTable, PolyphenyDbCatalogReader catalogReader, JavaTypeFactory typeFactory, Conformance conformance ) {
        return new PolyphenyDbSqlValidator( operatorTable, catalogReader, typeFactory, conformance );
    }


    @Override
    public ParserFactory getFactory( QueryLanguage language ) {
        return SqlParserImpl.FACTORY;
    }


    @Override
    public Parser getParser( QueryLanguage language, Reader reader, ParserConfig sqlParserConfig ) {
        SqlAbstractParserImpl parser = (SqlAbstractParserImpl) sqlParserConfig.parserFactory().getParser( reader );

        return new SqlParser( parser, sqlParserConfig );
    }


    @Override
    public OperatorTable getOracleOperatorTable() {
        return OracleSqlOperatorTable.instance();
    }


    @Override
    public Logger getLogger( QueryLanguage language, Class<RelNode> relNodeClass ) {
        return SqlToRelConverter.SQL2REL_LOGGER;
    }


    @Override
    public Identifier createIdentifier( QueryLanguage language, String name, ParserPos zero ) {
        return new SqlIdentifier( name, zero );
    }


    @Override
    public Identifier createIdentifier( QueryLanguage language, List<String> names, ParserPos zero ) {
        return new SqlIdentifier( names, zero );
    }


    @Override
    public DataTypeSpec createDataTypeSpec( QueryLanguage language, Identifier typeIdentifier, int precision, int scale, String charSetName, TimeZone o, ParserPos pos ) {
        return new SqlDataTypeSpec(
                (SqlIdentifier) typeIdentifier,
                precision,
                scale,
                charSetName,
                o,
                pos );
    }


    @Override
    public DataTypeSpec createDataTypeSpec( QueryLanguage language,
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
        return new SqlDataTypeSpec( (SqlIdentifier) typeIdentifier,
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


    @Override
    public IntervalQualifier createIntervalQualifier( QueryLanguage language,
            TimeUnit startUnit,
            int startPrecision,
            TimeUnit endUnit,
            int fractionalSecondPrecision,
            ParserPos zero ) {
        return new SqlIntervalQualifier( startUnit, startPrecision, endUnit, fractionalSecondPrecision, zero );
    }


    @Override
    public Literal createLiteral( QueryLanguage language, PolyType polyType, Object o, ParserPos pos ) {
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


    @Override
    public AggFunction createMinMaxAggFunction( QueryLanguage language, Kind kind ) {
        return new SqlMinMaxAggFunction( kind );
    }


    @Override
    public AggFunction createSumEmptyIsZeroFunction( QueryLanguage language ) {
        return new SqlSumEmptyIsZeroAggFunction();
    }


    @Override
    public AggFunction createBitOpAggFunction( QueryLanguage language, Kind kind ) {
        return new SqlBitOpAggFunction( kind );
    }


    @Override
    public AggFunction createSumAggFunction( QueryLanguage language, RelDataType type ) {
        return new SqlSumAggFunction( type );
    }


    @Override
    public Operator createFunction( QueryLanguage language, String name,
            Kind kind,
            PolyReturnTypeInference returnTypeInference,
            PolyOperandTypeInference o,
            PolySingleOperandTypeChecker typeChecker,
            FunctionCategory system ) {
        return new SqlFunction(
                name,
                kind,
                returnTypeInference, // returns boolean since we'll AND it
                o,
                typeChecker, // takes a numeric param
                system );
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
    public Operator createUserDefinedFunction( QueryLanguage language,
            Identifier name,
            PolyReturnTypeInference infer,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            List<RelDataType> paramTypes,
            Function function ) {
        return new SqlUserDefinedFunction( (SqlIdentifier) name, infer, explicit, typeChecker, paramTypes, function );
    }


    @Override
    public Operator createUserDefinedAggFunction( QueryLanguage language,
            Identifier name,
            PolyReturnTypeInference infer,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            AggregateFunction function,
            boolean requiresOrder,
            boolean requiresOver,
            Optionality optionality,
            RelDataTypeFactory typeFactory ) {
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


    @Override
    public Operator createUserDefinedTableMacro( QueryLanguage language,
            Identifier name,
            PolyReturnTypeInference typeInference,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            List<RelDataType> paramTypes,
            TableMacro function ) {
        return new SqlUserDefinedTableMacro( (SqlIdentifier) name, typeInference, explicit, typeChecker, paramTypes, function );
    }


    @Override
    public Operator createUserDefinedTableFunction( QueryLanguage sql,
            Identifier name,
            PolyReturnTypeInference typeInference,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            List<RelDataType> paramTypes,
            TableFunction function ) {
        return new SqlUserDefinedTableFunction( (SqlIdentifier) name, typeInference, explicit, typeChecker, paramTypes, function );
    }


}
