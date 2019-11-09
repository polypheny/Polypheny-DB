/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.fun;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlAggFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.OperandTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ReturnTypes;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SameOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlReturnTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlSingleOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeFamily;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeTransform;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeTransforms;
import ch.unibas.dmi.dbis.polyphenydb.util.Optionality;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * <code>LEAD</code> and <code>LAG</code> aggregate functions return the value of given expression evaluated at given offset.
 */
public class SqlLeadLagAggFunction extends SqlAggFunction {

    private static final SqlSingleOperandTypeChecker OPERAND_TYPES =
            OperandTypes.or(
                    OperandTypes.ANY,
                    OperandTypes.family( SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC ),
                    OperandTypes.and(
                            OperandTypes.family( SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.ANY ),
                            // Arguments 1 and 3 must have same type
                            new SameOperandTypeChecker( 3 ) {
                                @Override
                                protected List<Integer>
                                getOperandList( int operandCount ) {
                                    return ImmutableList.of( 0, 2 );
                                }
                            } ) );

    private static final SqlReturnTypeInference RETURN_TYPE =
            ReturnTypes.cascade( ReturnTypes.ARG0, ( binding, type ) -> {
                // Result is NOT NULL if NOT NULL default value is provided
                SqlTypeTransform transform;
                if ( binding.getOperandCount() < 3 ) {
                    transform = SqlTypeTransforms.FORCE_NULLABLE;
                } else {
                    RelDataType defValueType = binding.getOperandType( 2 );
                    transform = defValueType.isNullable()
                            ? SqlTypeTransforms.FORCE_NULLABLE
                            : SqlTypeTransforms.TO_NOT_NULLABLE;
                }
                return transform.transformType( binding, type );
            } );


    public SqlLeadLagAggFunction( SqlKind kind ) {
        super(
                kind.name(),
                null,
                kind,
                RETURN_TYPE,
                null,
                OPERAND_TYPES,
                SqlFunctionCategory.NUMERIC,
                false,
                true,
                Optionality.FORBIDDEN );
        Preconditions.checkArgument( kind == SqlKind.LEAD || kind == SqlKind.LAG );
    }


    @Override
    public boolean allowsFraming() {
        return false;
    }

}

