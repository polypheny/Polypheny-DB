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


import java.util.Locale;
import java.util.Objects;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlAggFunction;
import org.polypheny.db.sql.SqlCall;
import org.polypheny.db.sql.SqlFunctionCategory;
import org.polypheny.db.sql.SqlJsonConstructorNullClause;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.sql.validate.SqlValidatorImpl;
import org.polypheny.db.sql.validate.SqlValidatorScope;
import org.polypheny.db.type.OperandTypes;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.ReturnTypes;
import org.polypheny.db.util.Optionality;


/**
 * The <code>JSON_OBJECTAGG</code> aggregate function.
 */
public class SqlJsonObjectAggAggFunction extends SqlAggFunction {

    private final SqlJsonConstructorNullClause nullClause;


    /**
     * Creates a SqlJsonObjectAggAggFunction.
     */
    public SqlJsonObjectAggAggFunction( String name, SqlJsonConstructorNullClause nullClause ) {
        super(
                name,
                null,
                SqlKind.JSON_OBJECTAGG,
                ReturnTypes.VARCHAR_2000,
                null,
                OperandTypes.family( PolyTypeFamily.CHARACTER, PolyTypeFamily.ANY ),
                SqlFunctionCategory.SYSTEM,
                false,
                false,
                Optionality.FORBIDDEN );
        this.nullClause = Objects.requireNonNull( nullClause );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        assert call.operandCount() == 2;
        final SqlWriter.Frame frame = writer.startFunCall( "JSON_OBJECTAGG" );
        writer.keyword( "KEY" );
        call.operand( 0 ).unparse( writer, leftPrec, rightPrec );
        writer.keyword( "VALUE" );
        call.operand( 1 ).unparse( writer, leftPrec, rightPrec );
        writer.keyword( nullClause.sql );
        writer.endFunCall( frame );
    }


    @Override
    public RelDataType deriveType( SqlValidator validator, SqlValidatorScope scope, SqlCall call ) {
        // To prevent operator rewriting by SqlFunction#deriveType.
        for ( SqlNode operand : call.getOperandList() ) {
            RelDataType nodeType = validator.deriveType( scope, operand );
            ((SqlValidatorImpl) validator).setValidatedNodeType( operand, nodeType );
        }
        return validateOperands( validator, scope, call );
    }


    @Override
    public String toString() {
        return getName() + String.format( Locale.ROOT, "<%s>", nullClause );
    }


    public SqlJsonObjectAggAggFunction with( SqlJsonConstructorNullClause nullClause ) {
        return this.nullClause == nullClause
                ? this
                : new SqlJsonObjectAggAggFunction( getName(), nullClause );
    }


    public SqlJsonConstructorNullClause getNullClause() {
        return nullClause;
    }
}

