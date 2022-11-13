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

import org.apache.calcite.avatica.util.TimeUnit;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.RexConvertletTable;
import org.polypheny.db.languages.UnsupportedLanguageOperation;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlIntervalQualifier;
import org.polypheny.db.sql.language.fun.OracleSqlOperatorTable;
import org.polypheny.db.sql.language.fun.SqlBitOpAggFunction;
import org.polypheny.db.sql.language.fun.SqlMinMaxAggFunction;
import org.polypheny.db.sql.language.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.language.fun.SqlSumAggFunction;
import org.polypheny.db.sql.language.fun.SqlSumEmptyIsZeroAggFunction;
import org.polypheny.db.type.checker.PolySingleOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.StandardConvertletTable;


public class LanguageManagerImpl extends LanguageManager {




    @Override
    public RexConvertletTable getStandardConvertlet() {
        return StandardConvertletTable.INSTANCE;
    }


    @Override
    public OperatorTable getStdOperatorTable() {
        return SqlStdOperatorTable.instance();
    }


    @Override
    public OperatorTable getOracleOperatorTable() {
        return OracleSqlOperatorTable.instance();
    }


    @Override
    public Identifier createIdentifier( QueryLanguage language, String name, ParserPos zero ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new SqlIdentifier( name, zero );
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
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new SqlIntervalQualifier( startUnit, startPrecision, endUnit, fractionalSecondPrecision, zero );
        }
        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public AggFunction createMinMaxAggFunction( QueryLanguage language, Kind kind ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new SqlMinMaxAggFunction( kind );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public AggFunction createSumEmptyIsZeroFunction( QueryLanguage language ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new SqlSumEmptyIsZeroAggFunction();
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public AggFunction createBitOpAggFunction( QueryLanguage language, Kind kind ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
            return new SqlBitOpAggFunction( kind );
        }

        throw new UnsupportedLanguageOperation( language );
    }


    @Override
    public AggFunction createSumAggFunction( QueryLanguage language, AlgDataType type ) {
        if ( language == QueryLanguage.from( "sql" ) ) {
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


}
