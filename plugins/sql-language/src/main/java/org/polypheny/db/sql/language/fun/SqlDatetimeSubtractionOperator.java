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


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlSyntax;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * A special operator for the subtraction of two DATETIMEs. The format of DATETIME subtraction is:
 *
 * <blockquote><code>"(" &lt;datetime&gt; "-" &lt;datetime&gt; ")" &lt;interval qualifier&gt;</code></blockquote>
 *
 * This operator is special since it needs to hold the additional interval qualifier specification, when in {@link SqlCall} form.
 * In {@link RexNode} form, it has only two parameters, and the return type describes the desired type of interval.
 */
public class SqlDatetimeSubtractionOperator extends SqlSpecialOperator {


    public SqlDatetimeSubtractionOperator() {
        super(
                "-",
                Kind.MINUS,
                40,
                true,
                ReturnTypes.ARG2_NULLABLE,
                InferTypes.FIRST_KNOWN,
                OperandTypes.MINUS_DATE_OPERATOR );
    }


    @Override
    public SqlSyntax getSqlSyntax() {
        return SqlSyntax.SPECIAL;
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        writer.getDialect().unparseSqlDatetimeArithmetic( writer, call, Kind.MINUS, leftPrec, rightPrec );
    }


    @Override
    public Monotonicity getMonotonicity( OperatorBinding call ) {
        return OperatorRegistry.get( OperatorName.MINUS ).getMonotonicity( call );
    }

}

