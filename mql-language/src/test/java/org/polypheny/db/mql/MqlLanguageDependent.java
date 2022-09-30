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

package org.polypheny.db.mql;

import java.io.Reader;
import java.util.List;
import java.util.TimeZone;
import org.apache.calcite.avatica.util.TimeUnit;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
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
import org.polypheny.db.type.PolyIntervalQualifier;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.FamilyOperandTypeChecker;
import org.polypheny.db.type.checker.PolySingleOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.Optionality;
import org.slf4j.Logger;

public class MqlLanguageDependent extends LanguageManager {

    static {
        if ( !SqlRegisterer.isInit() ) {
            SqlRegisterer.registerOperators();
        }
        if ( !MqlRegisterer.isInit() ) {
            MqlRegisterer.registerOperators();
        }
    }


    @Override
    public Validator createValidator( QueryLanguage language, Context context, PolyphenyDbCatalogReader catalogReader ) {

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public NodeToAlgConverter createToRelConverter(
            QueryLanguage language,
            Validator validator,
            CatalogReader catalogReader,
            AlgOptCluster cluster,
            RexConvertletTable convertletTable,
            Config config ) {

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public RexConvertletTable getStandardConvertlet() {
        throw new UnsupportedLanguageOperation( QueryLanguage.SQL );
    }


    @Override
    public OperatorTable getStdOperatorTable() {
        throw new UnsupportedLanguageOperation( QueryLanguage.SQL );
    }


    @Override
    public Validator createPolyphenyValidator(
            QueryLanguage language,
            OperatorTable operatorTable,
            PolyphenyDbCatalogReader catalogReader,
            JavaTypeFactory typeFactory,
            Conformance conformance ) {

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public ParserFactory getFactory( QueryLanguage language ) {
        if ( language == QueryLanguage.MONGO_QL ) {
            return org.polypheny.db.mql.parser.impl.MqlParserImpl.FACTORY;
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public Parser getParser( QueryLanguage language, Reader reader, ParserConfig parserConfig ) {

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public OperatorTable getOracleOperatorTable() {
        throw new UnsupportedLanguageOperation( QueryLanguage.SQL );
    }


    @Override
    public Logger getLogger( QueryLanguage language, Class<AlgNode> algNodeClass ) {

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public Identifier createIdentifier( QueryLanguage language, String name, ParserPos zero ) {

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
        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public Literal createLiteral( QueryLanguage language, PolyType polyType, Object o, ParserPos pos ) {

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public AggFunction createMinMaxAggFunction( QueryLanguage language, Kind kind ) {

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public AggFunction createSumEmptyIsZeroFunction( QueryLanguage language ) {

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public AggFunction createBitOpAggFunction( QueryLanguage language, Kind kind ) {

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public AggFunction createSumAggFunction( QueryLanguage language, AlgDataType type ) {

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

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public void createIntervalTypeString( StringBuilder sb, PolyIntervalQualifier intervalQualifier ) {
        throw new UnsupportedLanguageOperation( QueryLanguage.SQL );
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

        throw new UnsupportedLanguageOperation( language );
    }


}