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

package org.polypheny.db.languages;

import java.io.Reader;
import java.util.List;
import java.util.TimeZone;
import lombok.Getter;
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
import org.polypheny.db.languages.Parser.ParserConfig;
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
import org.polypheny.db.type.PolyIntervalQualifier;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.FamilyOperandTypeChecker;
import org.polypheny.db.type.checker.PolySingleOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.Optionality;
import org.slf4j.Logger;

/**
 * LanguageManager is responsible for providing a way of accessing objects and functions of the different available languages.
 */
public abstract class LanguageManager {

    @Getter
    private static LanguageManager instance;


    public static synchronized LanguageManager setAndGetInstance( LanguageManager manager ) {
        if ( manager != null ) {
            instance = manager;
        }

        return instance;
    }


    public abstract Validator createValidator( QueryLanguage language, Context context, PolyphenyDbCatalogReader catalogReader );

    public abstract NodeToAlgConverter createToRelConverter(
            QueryLanguage sql,
            Validator validator,
            CatalogReader catalogReader,
            AlgOptCluster cluster,
            RexConvertletTable convertletTable,
            NodeToAlgConverter.Config config );

    public abstract RexConvertletTable getStandardConvertlet();

    public abstract OperatorTable getStdOperatorTable();

    public abstract Validator createPolyphenyValidator(
            QueryLanguage language,
            OperatorTable operatorTable,
            PolyphenyDbCatalogReader catalogReader,
            JavaTypeFactory typeFactory,
            Conformance conformance );

    public abstract ParserFactory getFactory( QueryLanguage language );

    public abstract Parser getParser( QueryLanguage language, Reader reader, ParserConfig sqlParserConfig );

    public abstract OperatorTable getOracleOperatorTable();

    public abstract Logger getLogger( QueryLanguage language, Class<AlgNode> algNodeClass );

    public abstract Identifier createIdentifier( QueryLanguage language, String name, ParserPos zero );

    public abstract DataTypeSpec createDataTypeSpec(
            QueryLanguage language,
            Identifier typeIdentifier,
            int precision,
            int scale,
            String charSetName,
            TimeZone o,
            ParserPos zero );

    public abstract DataTypeSpec createDataTypeSpec(
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
            ParserPos zero );

    public abstract IntervalQualifier createIntervalQualifier(
            QueryLanguage language,
            TimeUnit startUnit,
            int startPrecision,
            TimeUnit endUnit,
            int fractionalSecondPrecision,
            ParserPos zero );

    public abstract Literal createLiteral( QueryLanguage language, PolyType polyType, Object o, ParserPos pos );

    public abstract AggFunction createMinMaxAggFunction( QueryLanguage language, Kind kind );

    public abstract AggFunction createSumEmptyIsZeroFunction( QueryLanguage language );

    public abstract AggFunction createBitOpAggFunction( QueryLanguage language, Kind kind );

    public abstract AggFunction createSumAggFunction( QueryLanguage language, AlgDataType type );

    public abstract Operator createFunction(
            QueryLanguage language,
            String artificial_selectivity,
            Kind otherFunction,
            PolyReturnTypeInference aBoolean,
            PolyOperandTypeInference o,
            PolySingleOperandTypeChecker numeric,
            FunctionCategory system );

    public abstract void createIntervalTypeString( StringBuilder sb, PolyIntervalQualifier intervalQualifier );

    public abstract Operator createUserDefinedFunction(
            QueryLanguage language,
            Identifier name,
            PolyReturnTypeInference infer,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            List<AlgDataType> paramTypes,
            Function function );

    public abstract Operator createUserDefinedAggFunction(
            QueryLanguage language,
            Identifier name,
            PolyReturnTypeInference infer,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            AggregateFunction function,
            boolean b,
            boolean b1,
            Optionality forbidden,
            AlgDataTypeFactory typeFactory );

    public abstract Operator createUserDefinedTableMacro(
            QueryLanguage language,
            Identifier name,
            PolyReturnTypeInference cursor,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            List<AlgDataType> paramTypes,
            TableMacro function );

    public abstract Operator createUserDefinedTableFunction(
            QueryLanguage sql,
            Identifier name,
            PolyReturnTypeInference cursor,
            PolyOperandTypeInference explicit,
            FamilyOperandTypeChecker typeChecker,
            List<AlgDataType> paramTypes,
            TableFunction function );

}
