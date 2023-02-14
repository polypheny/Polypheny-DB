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

package org.polypheny.db.sql.language;


import java.util.List;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;


/**
 * Placeholder for an unresolved function.
 *
 * Created by the parser, then it is rewritten to proper SqlFunction by the validator to a function defined in a Polypheny-DB schema.
 */
public class SqlUnresolvedFunction extends SqlFunction {

    /**
     * Creates a placeholder SqlUnresolvedFunction for an invocation of a function with a possibly qualified name. This name must be resolved into either a builtin function or a user-defined function.
     *
     * @param sqlIdentifier possibly qualified identifier for function
     * @param returnTypeInference strategy to use for return type inference
     * @param operandTypeInference strategy to use for parameter type inference
     * @param operandTypeChecker strategy to use for parameter type checking
     * @param paramTypes array of parameter types
     * @param funcType function category
     */
    public SqlUnresolvedFunction( SqlIdentifier sqlIdentifier, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker, List<AlgDataType> paramTypes, FunctionCategory funcType ) {
        super( sqlIdentifier, returnTypeInference, operandTypeInference, operandTypeChecker, paramTypes, funcType );
    }


    /**
     * {@inheritDoc}T
     *
     * The operator class for this function isn't resolved to the correct class. This happens in the case of user defined functions. Return the return type to be 'ANY', so we don't fail.
     */
    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        final AlgDataTypeFactory typeFactory = opBinding.getTypeFactory();
        return typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.ANY ), true );
    }

}

