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

package org.polypheny.db.languages.sql.fun;


import org.apache.calcite.avatica.util.TimeUnit;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.Monotonicity;
import org.polypheny.db.languages.sql.SqlCall;
import org.polypheny.db.languages.sql.SqlOperatorBinding;
import org.polypheny.db.languages.sql.SqlSpecialOperator;
import org.polypheny.db.languages.sql.SqlSyntax;
import org.polypheny.db.languages.sql.SqlWriter;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.type.IntervalPolyType;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * Operator that adds an INTERVAL to a DATETIME.
 */
public class SqlDatetimePlusOperator extends SqlSpecialOperator {


    SqlDatetimePlusOperator() {
        super(
                "+",
                Kind.PLUS,
                40,
                true,
                ReturnTypes.ARG2_NULLABLE,
                InferTypes.FIRST_KNOWN,
                OperandTypes.MINUS_DATE_OPERATOR );
    }


    @Override
    public RelDataType inferReturnType( SqlOperatorBinding opBinding ) {
        final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final RelDataType leftType = opBinding.getOperandType( 0 );
        final IntervalPolyType unitType = (IntervalPolyType) opBinding.getOperandType( 1 );
        final TimeUnit timeUnit = unitType.getIntervalQualifier().getStartUnit();
        return SqlTimestampAddFunction.deduceType( typeFactory, timeUnit, unitType, leftType );
    }


    @Override
    public SqlSyntax getSqlSyntax() {
        return SqlSyntax.SPECIAL;
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        writer.getDialect().unparseSqlDatetimeArithmetic( writer, call, Kind.PLUS, leftPrec, rightPrec );
    }


    @Override
    public Monotonicity getMonotonicity( SqlOperatorBinding call ) {
        return SqlStdOperatorTable.PLUS.getMonotonicity( call );
    }
}

