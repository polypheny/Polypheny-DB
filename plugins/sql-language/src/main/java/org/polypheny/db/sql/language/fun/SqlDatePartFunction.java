/*
 * Copyright 2019-2023 The Polypheny Project
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


import java.util.List;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlCallBinding;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.sql.language.SqlIntervalQualifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.type.OperandCountRange;
import org.polypheny.db.type.PolyOperandCountRanges;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.temporal.TimeUnit;


/**
 * SqlDatePartFunction represents the SQL:1999 standard {@code YEAR}, {@code QUARTER}, {@code MONTH} and {@code DAY} functions.
 */
public class SqlDatePartFunction extends SqlFunction {

    private final TimeUnit timeUnit;


    public SqlDatePartFunction( String name, TimeUnit timeUnit ) {
        super(
                name,
                Kind.OTHER,
                ReturnTypes.BIGINT_NULLABLE,
                InferTypes.FIRST_KNOWN,
                OperandTypes.DATETIME,
                FunctionCategory.TIMEDATE );
        this.timeUnit = timeUnit;
    }


    @Override
    public SqlNode rewriteCall( SqlValidator validator, SqlCall call ) {
        final List<Node> operands = call.getOperandList();
        final ParserPos pos = call.getPos();
        return (SqlNode) OperatorRegistry.get( OperatorName.EXTRACT ).createCall(
                pos,
                new SqlIntervalQualifier( timeUnit, null, ParserPos.ZERO ),
                operands.get( 0 ) );
    }


    @Override
    public OperandCountRange getOperandCountRange() {
        return PolyOperandCountRanges.of( 1 );
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        assert 1 == operandsCount;
        return "{0}({1})";
    }


    @Override
    public boolean checkOperandTypes( SqlCallBinding callBinding, boolean throwOnFailure ) {
        return OperandTypes.DATETIME.checkSingleOperandType( callBinding, callBinding.operand( 0 ), 0, throwOnFailure );
    }

}

