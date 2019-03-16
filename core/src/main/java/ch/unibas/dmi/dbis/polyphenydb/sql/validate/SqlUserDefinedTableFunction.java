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

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.schema.TableFunction;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlReturnTypeInference;
import java.lang.reflect.Type;
import java.util.List;


/**
 * User-defined table function.
 *
 * Created by the validator, after resolving a function call to a function defined in a Polypheny-DB schema.
 */
public class SqlUserDefinedTableFunction extends SqlUserDefinedFunction {

    public SqlUserDefinedTableFunction( SqlIdentifier opName, SqlReturnTypeInference returnTypeInference, SqlOperandTypeInference operandTypeInference, SqlOperandTypeChecker operandTypeChecker, List<RelDataType> paramTypes, TableFunction function ) {
        super(
                opName,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                paramTypes,
                function,
                SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION );
    }


    /**
     * Returns function that implements given operator call.
     *
     * @return function that implements given operator call
     */
    public TableFunction getFunction() {
        return (TableFunction) super.getFunction();
    }


    /**
     * Returns the record type of the table yielded by this function when applied to given arguments. Only literal arguments are passed, non-literal are replaced with default values (null, 0, false, etc).
     *
     * @param typeFactory Type factory
     * @param operandList arguments of a function call (only literal arguments are passed, nulls for non-literal ones)
     * @return row type of the table
     */
    public RelDataType getRowType( RelDataTypeFactory typeFactory, List<SqlNode> operandList ) {
        List<Object> arguments = SqlUserDefinedTableMacro.convertArguments( typeFactory, operandList, function, getNameAsId(), false );
        return getFunction().getRowType( typeFactory, arguments );
    }


    /**
     * Returns the row type of the table yielded by this function when applied to given arguments. Only literal arguments are passed, non-literal are replaced with default values (null, 0, false, etc).
     *
     * @param operandList arguments of a function call (only literal arguments are passed, nulls for non-literal ones)
     * @return element type of the table (e.g. {@code Object[].class})
     */
    public Type getElementType( RelDataTypeFactory typeFactory, List<SqlNode> operandList ) {
        List<Object> arguments = SqlUserDefinedTableMacro.convertArguments( typeFactory, operandList, function, getNameAsId(), false );
        return getFunction().getElementType( arguments );
    }
}
