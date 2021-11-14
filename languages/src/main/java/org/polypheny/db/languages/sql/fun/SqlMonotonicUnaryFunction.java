/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.languages.sql.fun;


import org.polypheny.db.core.FunctionCategory;
import org.polypheny.db.core.Kind;
import org.polypheny.db.core.Monotonicity;
import org.polypheny.db.languages.sql.SqlFunction;
import org.polypheny.db.languages.sql.SqlOperatorBinding;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;


/**
 * Base class for unary operators such as FLOOR/CEIL which are monotonic for monotonic inputs.
 */
public class SqlMonotonicUnaryFunction extends SqlFunction {


    protected SqlMonotonicUnaryFunction( String name, Kind kind, PolyReturnTypeInference returnTypeInference, PolyOperandTypeInference operandTypeInference, PolyOperandTypeChecker operandTypeChecker, FunctionCategory funcType ) {
        super( name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, funcType );
    }


    @Override
    public Monotonicity getMonotonicity( SqlOperatorBinding call ) {
        return call.getOperandMonotonicity( 0 ).unstrict();
    }
}

