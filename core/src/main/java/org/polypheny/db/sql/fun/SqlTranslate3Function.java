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


import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlFunction;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.type.OperandTypes;
import org.polypheny.db.sql.type.ReturnTypes;


/**
 * Definition of the "TRANSLATE" built-in SQL function that takes 3 arguments.
 *
 * Based on Oracle's {@code TRANSLATE} function, it is commonly called "TRANSLATE3" to distinguish it from the standard SQL function {@link SqlStdOperatorTable#TRANSLATE} that takes 2 arguments
 * and has an entirely different purpose.
 */
public class SqlTranslate3Function extends SqlFunction {

    /**
     * Creates the SqlTranslate3Function.
     */
    SqlTranslate3Function() {
        super(
                "TRANSLATE3",
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.ARG0_NULLABLE_VARYING,
                null,
                OperandTypes.STRING_STRING_STRING,
                SqlFunctionCategory.STRING );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startFunCall( "TRANSLATE" );
        for ( SqlNode sqlNode : call.getOperandList() ) {
            writer.sep( "," );
            sqlNode.unparse( writer, leftPrec, rightPrec );
        }
        writer.endFunCall( frame );
    }


    @Override
    public String getSignatureTemplate( final int operandsCount ) {
        if ( 3 == operandsCount ) {
            return "{0}({1}, {2}, {3})";
        }
        throw new AssertionError();
    }

}

