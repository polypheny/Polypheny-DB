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

package ch.unibas.dmi.dbis.polyphenydb.sql.fun;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlAggFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlJsonConstructorNullClause;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFamily;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import ch.unibas.dmi.dbis.polyphenydb.util.Optionality;
import java.util.Locale;
import java.util.Objects;


/**
 * The <code>JSON_OBJECTAGG</code> aggregate function.
 */
public class SqlJsonArrayAggAggFunction extends SqlAggFunction {

    private final SqlJsonConstructorNullClause nullClause;


    public SqlJsonArrayAggAggFunction( String name, SqlJsonConstructorNullClause nullClause ) {
        super(
                name,
                null,
                SqlKind.JSON_ARRAYAGG,
                ReturnTypes.VARCHAR_2000,
                null,
                OperandTypes.family( SqlTypeFamily.ANY ),
                SqlFunctionCategory.SYSTEM,
                false,
                false,
                Optionality.FORBIDDEN );
        this.nullClause = Objects.requireNonNull( nullClause );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        assert call.operandCount() == 1;
        final SqlWriter.Frame frame = writer.startFunCall( "JSON_ARRAYAGG" );
        call.operand( 0 ).unparse( writer, leftPrec, rightPrec );
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


    public SqlJsonArrayAggAggFunction with( SqlJsonConstructorNullClause nullClause ) {
        return this.nullClause == nullClause
                ? this
                : new SqlJsonArrayAggAggFunction( getName(), nullClause );
    }


    public SqlJsonConstructorNullClause getNullClause() {
        return nullClause;
    }
}

