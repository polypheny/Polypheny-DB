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

package org.polypheny.db.sql.language.fun;


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.IntervalPolyType;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.temporal.TimeUnit;


/**
 * Operator that adds an INTERVAL to a DATETIME.
 */
public class SqlDatetimePlusOperator extends SqlSpecialOperator {


    public SqlDatetimePlusOperator() {
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
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
        final AlgDataType leftType = opBinding.getOperandType( 0 );
        final IntervalPolyType unitType = (IntervalPolyType) opBinding.getOperandType( 1 );
        final TimeUnit timeUnit = unitType.getIntervalQualifier().getTimeUnitRange().startUnit;
        return SqlTimestampAddFunction.deduceType( typeFactory, timeUnit, unitType, leftType );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        writer.getDialect().unparseSqlDatetimeArithmetic( writer, call, Kind.PLUS, leftPrec, rightPrec );
    }


    @Override
    public Monotonicity getMonotonicity( OperatorBinding call ) {
        return OperatorRegistry.get( OperatorName.PLUS ).getMonotonicity( call );
    }

}

