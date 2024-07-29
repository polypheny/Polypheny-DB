/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.sql.language.util;

import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.languages.NodeToAlgConverter;
import org.polypheny.db.languages.NodeToAlgConverter.Config;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.RexConvertletTable;
import org.polypheny.db.languages.UnsupportedLanguageOperation;
import org.polypheny.db.nodes.DataTypeSpec;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.nodes.OperatorImpl;
import org.polypheny.db.nodes.validate.Validator;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.schema.AggregateFunction;
import org.polypheny.db.schema.Function;
import org.polypheny.db.schema.FunctionParameter;
import org.polypheny.db.schema.ScalarFunction;
import org.polypheny.db.schema.TableFunction;
import org.polypheny.db.schema.TableMacro;
import org.polypheny.db.schema.impl.ScalarFunctionImpl;
import org.polypheny.db.sql.language.SqlDataTypeSpec;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlIntervalQualifier;
import org.polypheny.db.sql.language.dialect.AnsiSqlDialect;
import org.polypheny.db.sql.language.fun.SqlBitOpAggFunction;
import org.polypheny.db.sql.language.fun.SqlMinMaxAggFunction;
import org.polypheny.db.sql.language.fun.SqlSumAggFunction;
import org.polypheny.db.sql.language.fun.SqlSumEmptyIsZeroAggFunction;
import org.polypheny.db.sql.language.parser.SqlAbstractParserImpl;
import org.polypheny.db.sql.language.parser.SqlParser;
import org.polypheny.db.sql.language.pretty.SqlPrettyWriter;
import org.polypheny.db.sql.language.validate.PolyphenyDbSqlValidator;
import org.polypheny.db.sql.language.validate.SqlUserDefinedAggFunction;
import org.polypheny.db.sql.language.validate.SqlUserDefinedFunction;
import org.polypheny.db.sql.language.validate.SqlUserDefinedTableFunction;
import org.polypheny.db.sql.language.validate.SqlUserDefinedTableMacro;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.sql2alg.SqlRexConvertletTable;
import org.polypheny.db.sql.sql2alg.SqlToAlgConverter;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyIntervalQualifier;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.checker.FamilyOperandTypeChecker;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.checker.PolySingleOperandTypeChecker;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.Optionality;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.temporal.TimeUnit;
import org.slf4j.Logger;

public class SqlTypeUtil {


    public static Identifier createIdentifier( String name, ParserPos zero ) {
        return new SqlIdentifier( name, zero );
    }


    public Identifier getSqlIdentifier( AlgDataType type ) {
        PolyType typeName = type.getPolyType();
        if ( typeName == null ) {
            return null;
        }
        return SqlTypeUtil.createIdentifier( typeName.name(), ParserPos.ZERO );
    }


    /**
     * Converts an instance of AlgDataType to an instance of SqlDataTypeSpec.
     *
     * @param type type descriptor
     * @return corresponding parse representation
     */
    public static DataTypeSpec convertTypeToSpec( AlgDataType type ) {
        PolyType typeName = type.getPolyType();

        // TODO jvs: support row types, user-defined types, interval types, multiset types, etc
        assert typeName != null;
        Identifier typeIdentifier = SqlTypeUtil.createIdentifier( typeName.name(), ParserPos.ZERO );

        String charSetName = null;

        if ( inCharFamily( type ) ) {
            charSetName = type.getCharset().name();
            // TODO jvs: collation
        }

        // REVIEW jvs: discriminate between precision/scale zero and unspecified?

        // REVIEW angel: Use neg numbers to indicate unspecified precision/scale

        if ( typeName.allowsScale() ) {
            return createDataTypeSpec(
                    typeIdentifier,
                    type.getPrecision(),
                    type.getScale(),
                    charSetName,
                    null,
                    ParserPos.ZERO );
        } else if ( typeName.allowsPrec() ) {
            return createDataTypeSpec(
                    typeIdentifier,
                    type.getPrecision(),
                    -1,
                    charSetName,
                    null,
                    ParserPos.ZERO );
        } else if ( typeName.getFamily() == PolyTypeFamily.ARRAY ) {
            ArrayType arrayType = (ArrayType) type;
            Identifier componentTypeIdentifier = SqlTypeUtil.createIdentifier( arrayType.getComponentType().getPolyType().getName(), ParserPos.ZERO );
            return createDataTypeSpec(
                    typeIdentifier,
                    componentTypeIdentifier,
                    arrayType.getComponentType().getPrecision(),
                    arrayType.getComponentType().getScale(),
                    (int) arrayType.getDimension(),
                    (int) arrayType.getCardinality(),
                    charSetName,
                    null,
                    arrayType.isNullable(),
                    ParserPos.ZERO );
        } else {
            return createDataTypeSpec(
                    typeIdentifier,
                    -1,
                    -1,
                    charSetName,
                    null,
                    ParserPos.ZERO );
        }
    }


    public static DataTypeSpec createDataTypeSpec(
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


    public static DataTypeSpec createDataTypeSpec(
            Identifier typeIdentifier,
            int precision,
            int scale,
            String charSetName,
            TimeZone o,
            ParserPos pos ) {

        return new SqlDataTypeSpec(
                (SqlIdentifier) typeIdentifier,
                precision,
                scale,
                charSetName,
                o,
                pos );
    }


    /**
     * @return true if type is in SqlTypeFamily.Character
     */
    public static boolean inCharFamily( AlgDataType type ) {
        return type.getFamily() == PolyTypeFamily.CHARACTER;
    }


    public NodeToAlgConverter createToRelConverter(
            QueryLanguage language,
            Validator validator,
            Snapshot snapshot,
            AlgCluster cluster,
            RexConvertletTable convertletTable,
            Config config ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return getSqlToRelConverter( (SqlValidator) validator, snapshot, cluster, (SqlRexConvertletTable) convertletTable, config );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    private SqlToAlgConverter getSqlToRelConverter(
            SqlValidator validator,
            Snapshot snapshot,
            AlgCluster cluster,
            SqlRexConvertletTable convertletTable,
            Config config ) {
        return new SqlToAlgConverter( validator, snapshot, cluster, convertletTable, config );
    }


    public Validator createPolyphenyValidator(
            QueryLanguage language,
            OperatorTable operatorTable,
            Snapshot snapshot,
            JavaTypeFactory typeFactory,
            Conformance conformance ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new PolyphenyDbSqlValidator( operatorTable, snapshot, typeFactory, conformance );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    public Parser getParser( QueryLanguage language, Reader reader, ParserConfig parserConfig ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            SqlAbstractParserImpl parser = (SqlAbstractParserImpl) parserConfig.parserFactory().getParser( reader );
            return new SqlParser( parser, parserConfig );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    public Logger getLogger( QueryLanguage language, Class<AlgNode> algNodeClass ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return SqlToAlgConverter.SQL2REL_LOGGER;
        }

        throw new UnsupportedLanguageOperation( language );
    }


    public Identifier createIdentifier( QueryLanguage language, String name, ParserPos zero ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new SqlIdentifier( name, zero );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    public IntervalQualifier createIntervalQualifier(
            QueryLanguage language,
            TimeUnit startUnit,
            int startPrecision,
            TimeUnit endUnit,
            int fractionalSecondPrecision,
            ParserPos zero ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new SqlIntervalQualifier( startUnit, startPrecision, endUnit, fractionalSecondPrecision, zero );
        }
        throw new UnsupportedLanguageOperation( language );
    }


    public AggFunction createMinMaxAggFunction( QueryLanguage language, Kind kind ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new SqlMinMaxAggFunction( kind );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    public AggFunction createSumEmptyIsZeroFunction( QueryLanguage language ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new SqlSumEmptyIsZeroAggFunction();
        }

        throw new UnsupportedLanguageOperation( language );
    }


    public AggFunction createBitOpAggFunction( QueryLanguage language, Kind kind ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new SqlBitOpAggFunction( kind );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    public AggFunction createSumAggFunction( QueryLanguage language, AlgDataType type ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new SqlSumAggFunction( type );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    public Operator createFunction(
            QueryLanguage language,
            String name,
            Kind kind,
            PolyReturnTypeInference returnTypeInference,
            PolyOperandTypeInference o,
            PolySingleOperandTypeChecker typeChecker,
            FunctionCategory system ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
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


    public Operator createUserDefinedFunction(
            QueryLanguage language,
            Identifier name,
            PolyReturnTypeInference infer,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            List<AlgDataType> paramTypes,
            Function function ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new SqlUserDefinedFunction( (SqlIdentifier) name, infer, explicit, typeChecker, paramTypes, function );
        }

        throw new UnsupportedLanguageOperation( language );
    }


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
        if ( language == QueryLanguage.from( "sql" ) ) {
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


    public Operator createUserDefinedTableMacro(
            QueryLanguage language,
            Identifier name,
            PolyReturnTypeInference typeInference,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            List<AlgDataType> paramTypes,
            TableMacro function ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new SqlUserDefinedTableMacro( (SqlIdentifier) name, typeInference, explicit, typeChecker, paramTypes, function );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    public Operator createUserDefinedTableFunction(
            QueryLanguage language,
            Identifier name,
            PolyReturnTypeInference typeInference,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            List<AlgDataType> paramTypes,
            TableFunction function ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new SqlUserDefinedTableFunction( (SqlIdentifier) name, typeInference, explicit, typeChecker, paramTypes, function );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    /**
     * Converts a function to a {@link OperatorImpl}.
     *
     * The {@code typeFactory} argument is technical debt; see [POLYPHENYDB-2082] Remove RelDataTypeFactory argument from SqlUserDefinedAggFunction constructor.
     */
    private static Operator toOp( AlgDataTypeFactory typeFactory, Identifier name, final Function function ) {
        List<AlgDataType> argTypes = new ArrayList<>();
        List<PolyTypeFamily> typeFamilies = new ArrayList<>();
        for ( FunctionParameter o : function.getParameters() ) {
            final AlgDataType type = o.getType( typeFactory );
            argTypes.add( type );
            typeFamilies.add( Util.first( type.getPolyType().getFamily(), PolyTypeFamily.ANY ) );
        }
        final FamilyOperandTypeChecker typeChecker = OperandTypes.family( typeFamilies, i -> function.getParameters().get( i ).isOptional() );
        final List<AlgDataType> paramTypes = toSql( typeFactory, argTypes );
        if ( function instanceof ScalarFunction ) {
            return new SqlUserDefinedFunction( (SqlIdentifier) name, infer( (ScalarFunction) function ), InferTypes.explicit( argTypes ), typeChecker, paramTypes, function );
        } else if ( function instanceof AggregateFunction ) {
            return new SqlUserDefinedAggFunction(
                    (SqlIdentifier) name,
                    infer( (AggregateFunction) function ),
                    InferTypes.explicit( argTypes ),
                    typeChecker,
                    (AggregateFunction) function,
                    false,
                    false,
                    Optionality.FORBIDDEN,
                    typeFactory );
        } else if ( function instanceof TableMacro ) {
            return new SqlUserDefinedTableMacro( (SqlIdentifier) name, ReturnTypes.CURSOR, InferTypes.explicit( argTypes ), typeChecker, paramTypes, (TableMacro) function );
        } else if ( function instanceof TableFunction ) {
            return new SqlUserDefinedTableFunction( (SqlIdentifier) name, ReturnTypes.CURSOR, InferTypes.explicit( argTypes ), typeChecker, paramTypes, (TableFunction) function );
        } else {
            throw new AssertionError( "unknown function type " + function );
        }
    }


    private static PolyReturnTypeInference infer( final AggregateFunction function ) {
        return opBinding -> {
            final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
            final AlgDataType type = function.getReturnType( typeFactory );
            return toSql( typeFactory, type );
        };
    }


    private static List<AlgDataType> toSql( final AlgDataTypeFactory typeFactory, List<AlgDataType> types ) {
        return types.stream().map( type -> toSql( typeFactory, type ) ).collect( Collectors.toList() );
    }


    private static AlgDataType toSql( AlgDataTypeFactory typeFactory, AlgDataType type ) {
        if ( type instanceof AlgDataTypeFactoryImpl.JavaType && ((AlgDataTypeFactoryImpl.JavaType) type).getJavaClass() == Object.class ) {
            return typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.ANY ), true );
        }
        return JavaTypeFactoryImpl.toSql( typeFactory, type );
    }


    private static PolyReturnTypeInference infer( final ScalarFunction function ) {
        return opBinding -> {
            final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
            final AlgDataType type;
            if ( function instanceof ScalarFunctionImpl ) {
                type = ((ScalarFunctionImpl) function).getReturnType( typeFactory, opBinding );
            } else {
                type = function.getReturnType( typeFactory );
            }
            return toSql( typeFactory, type );
        };
    }

}
