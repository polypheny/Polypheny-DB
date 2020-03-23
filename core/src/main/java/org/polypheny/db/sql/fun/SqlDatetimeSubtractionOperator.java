/*
 * Copyright 2019-2020 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.sql.fun;


import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperatorBinding;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlSyntax;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.validate.SqlMonotonicity;
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
                SqlKind.MINUS,
                40,
                true,
                ReturnTypes.ARG2_NULLABLE,
                InferTypes.FIRST_KNOWN,
                OperandTypes.MINUS_DATE_OPERATOR );
    }


    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        writer.getDialect().unparseSqlDatetimeArithmetic( writer, call, SqlKind.MINUS, leftPrec, rightPrec );
    }


    @Override
    public SqlMonotonicity getMonotonicity( SqlOperatorBinding call ) {
        return SqlStdOperatorTable.MINUS.getMonotonicity( call );
    }
}

