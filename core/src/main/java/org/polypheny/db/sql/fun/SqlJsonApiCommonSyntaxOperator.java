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
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * The JSON API common syntax including a path specification, which is for JSON querying and processing.
 */
public class SqlJsonApiCommonSyntaxOperator extends SqlSpecialOperator {

    public SqlJsonApiCommonSyntaxOperator() {
        super(
                "JSON_API_COMMON_SYNTAX",
                SqlKind.JSON_API_COMMON_SYNTAX,
                100,
                true,
                ReturnTypes.explicit( PolyType.ANY ),
                null,
                OperandTypes.family( PolyTypeFamily.ANY, PolyTypeFamily.STRING ) );
    }


    @Override
    protected void checkOperandCount( SqlValidator validator, PolyOperandTypeChecker argType, SqlCall call ) {
        if ( call.operandCount() != 2 ) {
            throw new UnsupportedOperationException( "json passing syntax is not yet supported" );
        }
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.SIMPLE );
        call.operand( 0 ).unparse( writer, 0, 0 );
        writer.sep( ",", true );
        call.operand( 1 ).unparse( writer, 0, 0 );
        if ( call.operandCount() > 2 ) {
            writer.keyword( "PASSING" );
            for ( int i = 2; i < call.getOperandList().size(); i += 2 ) {
                call.operand( i ).unparse( writer, 0, 0 );
                writer.keyword( "AS" );
                call.operand( i + 1 ).unparse( writer, 0, 0 );
            }
        }
        writer.endFunCall( frame );
    }
}

