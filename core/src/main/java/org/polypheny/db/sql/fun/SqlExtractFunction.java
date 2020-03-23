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


import org.apache.calcite.avatica.util.TimeUnitRange;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperatorBinding;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.validate.SqlMonotonicity;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Util;


/**
 * The SQL <code>EXTRACT</code> operator. Extracts a specified field value from a DATETIME or an INTERVAL. E.g.<br>
 * <code>EXTRACT(HOUR FROM INTERVAL '364 23:59:59')</code> returns <code> 23</code>
 */
public class SqlExtractFunction extends SqlFunction {

    // SQL2003, Part 2, Section 4.4.3 - extract returns a exact numeric
    // TODO: Return type should be decimal for seconds
    public SqlExtractFunction() {
        super( "EXTRACT",
                SqlKind.EXTRACT,
                ReturnTypes.BIGINT_NULLABLE,
                null,
                OperandTypes.INTERVALINTERVAL_INTERVALDATETIME,
                SqlFunctionCategory.SYSTEM );
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        Util.discard( operandsCount );
        return "{0}({1} FROM {2})";
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( getName() );
        call.operand( 0 ).unparse( writer, 0, 0 );
        writer.sep( "FROM" );
        call.operand( 1 ).unparse( writer, 0, 0 );
        writer.endFunCall( frame );
    }


    @Override
    public SqlMonotonicity getMonotonicity( SqlOperatorBinding call ) {
        switch ( call.getOperandLiteralValue( 0, TimeUnitRange.class ) ) {
            case YEAR:
                return call.getOperandMonotonicity( 1 ).unstrict();
            default:
                return SqlMonotonicity.NOT_MONOTONIC;
        }
    }
}

