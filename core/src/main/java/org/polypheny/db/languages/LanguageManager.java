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

package org.polypheny.db.languages;

import java.io.Reader;
import java.util.TimeZone;
import org.apache.calcite.avatica.util.TimeUnit;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.core.AggFunction;
import org.polypheny.db.core.Conformance;
import org.polypheny.db.core.DataTypeSpec;
import org.polypheny.db.core.FunctionCategory;
import org.polypheny.db.core.Identifier;
import org.polypheny.db.core.IntervalQualifier;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.Literal;
import org.polypheny.db.core.Operator;
import org.polypheny.db.core.OperatorTable;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.core.Validator;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable.ViewExpander;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.PolySingleOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.slf4j.Logger;

public abstract class LanguageManager {

    private static LanguageManager instance;


    public static LanguageManager getInstance() {
        return instance;
    }


    public static synchronized LanguageManager setAndGetInstance( LanguageManager manager ) {
        instance = manager;
        return instance;
    }


    public abstract Validator createValidator( QueryLanguage language, Context context, PolyphenyDbCatalogReader catalogReader );

    public abstract Operator createSpecialOperator( String get, Kind otherFunction );

    public abstract NodeToRelConverter createToRelConverter( QueryLanguage sql,
            ViewExpander polyphenyDbPreparingStmt,
            Validator validator,
            CatalogReader catalogReader,
            RelOptCluster cluster,
            RexConvertletTable convertletTable,
            NodeToRelConverter.Config config );

    public abstract RexConvertletTable getStandardConvertlet();

    public abstract OperatorTable getStdOperatorTable();

    public abstract Validator createPolyphenyValidator( QueryLanguage sql, OperatorTable operatorTable, PolyphenyDbCatalogReader catalogReader, JavaTypeFactory typeFactory, Conformance conformance );

    public abstract ParserFactory getFactory( QueryLanguage sql );

    public abstract Parser getParser( QueryLanguage sql, Reader reader, ParserConfig sqlParserConfig );

    public abstract OperatorTable getOracleOperatorTable();

    public abstract Logger getLogger( QueryLanguage queryLanguage, Class<RelNode> relNodeClass );

    public abstract Identifier createIdentifier( QueryLanguage sql, String name, ParserPos zero );

    public abstract DataTypeSpec createDataTypeSpec( QueryLanguage sql, Identifier typeIdentifier, int precision, int scale, String charSetName, TimeZone o, ParserPos zero );

    public abstract DataTypeSpec createDataTypeSpec( QueryLanguage sql, Identifier typeIdentifier, Identifier componentTypeIdentifier, int precision, int scale, int dimension, int cardinality, String charSetName, TimeZone o, boolean nullable, ParserPos zero );

    public abstract IntervalQualifier createIntervalQualifier( QueryLanguage queryLanguage, TimeUnit startUnit, int startPrecision, TimeUnit endUnit, int fractionalSecondPrecision, ParserPos zero );

    public abstract Literal createLiteral( PolyType polyType, Object o, ParserPos pos );

    public abstract AggFunction createMinMaxAggFunction( Kind kind );

    public abstract AggFunction createSumEmptyIsZeroFunction();

    public abstract AggFunction createBitOpAggFunction( Kind kind );

    public abstract AggFunction createSumAggFunction( RelDataType type );

    public abstract Operator createFunction( String artificial_selectivity, Kind otherFunction, PolyReturnTypeInference aBoolean, PolyOperandTypeInference o, PolySingleOperandTypeChecker numeric, FunctionCategory system );

}
