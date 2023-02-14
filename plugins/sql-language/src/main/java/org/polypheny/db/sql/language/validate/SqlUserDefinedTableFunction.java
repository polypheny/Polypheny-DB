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

package org.polypheny.db.sql.language.validate;


import java.lang.reflect.Type;
import java.util.List;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.schema.TableFunction;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;


/**
 * User-defined table function.
 *
 * Created by the validator, after resolving a function call to a function defined in a Polypheny-DB schema.
 */
public class SqlUserDefinedTableFunction extends SqlUserDefinedFunction implements org.polypheny.db.algebra.fun.TableFunction {

    public SqlUserDefinedTableFunction( SqlIdentifier opName, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker, List<AlgDataType> paramTypes, TableFunction function ) {
        super(
                opName,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                paramTypes,
                function,
                FunctionCategory.USER_DEFINED_TABLE_FUNCTION );
    }


    /**
     * Returns function that implements given operator call.
     *
     * @return function that implements given operator call
     */
    @Override
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
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory, List<SqlNode> operandList ) {
        List<Object> arguments = SqlUserDefinedTableMacro.convertArguments( typeFactory, operandList, function, getNameAsId(), false );
        return getFunction().getRowType( typeFactory, arguments );
    }


    /**
     * Returns the row type of the table yielded by this function when applied to given arguments. Only literal arguments are passed, non-literal are replaced with default values (null, 0, false, etc).
     *
     * @param operandList arguments of a function call (only literal arguments are passed, nulls for non-literal ones)
     * @return element type of the table (e.g. {@code Object[].class})
     */
    public Type getElementType( AlgDataTypeFactory typeFactory, List<SqlNode> operandList ) {
        List<Object> arguments = SqlUserDefinedTableMacro.convertArguments( typeFactory, operandList, function, getNameAsId(), false );
        return getFunction().getElementType( arguments );
    }

}
