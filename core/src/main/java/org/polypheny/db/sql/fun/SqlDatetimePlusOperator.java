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
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorBinding;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSpecialOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSyntax;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.InferTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.IntervalSqlType;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlMonotonicity;
import org.apache.calcite.avatica.util.TimeUnit;


/**
 * Operator that adds an INTERVAL to a DATETIME.
 */
public class SqlDatetimePlusOperator extends SqlSpecialOperator {


    SqlDatetimePlusOperator() {
        super(
                "+",
                SqlKind.PLUS,
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
        final IntervalSqlType unitType = (IntervalSqlType) opBinding.getOperandType( 1 );
        final TimeUnit timeUnit = unitType.getIntervalQualifier().getStartUnit();
        return SqlTimestampAddFunction.deduceType( typeFactory, timeUnit, unitType, leftType );
    }


    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        writer.getDialect().unparseSqlDatetimeArithmetic( writer, call, SqlKind.PLUS, leftPrec, rightPrec );
    }


    @Override
    public SqlMonotonicity getMonotonicity( SqlOperatorBinding call ) {
        return SqlStdOperatorTable.PLUS.getMonotonicity( call );
    }
}

