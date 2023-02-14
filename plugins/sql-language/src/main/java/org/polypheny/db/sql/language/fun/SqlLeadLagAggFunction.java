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

package org.polypheny.db.sql.language.fun;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.sql.language.SqlAggFunction;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.PolyTypeTransform;
import org.polypheny.db.type.PolyTypeTransforms;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.checker.PolySingleOperandTypeChecker;
import org.polypheny.db.type.checker.SameOperandTypeChecker;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.type.inference.ReturnTypes;
import org.polypheny.db.util.Optionality;


/**
 * <code>LEAD</code> and <code>LAG</code> aggregate functions return the value of given expression evaluated at given offset.
 */
public class SqlLeadLagAggFunction extends SqlAggFunction {

    private static final PolySingleOperandTypeChecker OPERAND_TYPES =
            OperandTypes.or(
                    OperandTypes.ANY,
                    OperandTypes.family( PolyTypeFamily.ANY, PolyTypeFamily.NUMERIC ),
                    OperandTypes.and(
                            OperandTypes.family( PolyTypeFamily.ANY, PolyTypeFamily.NUMERIC, PolyTypeFamily.ANY ),
                            // Arguments 1 and 3 must have same type
                            new SameOperandTypeChecker( 3 ) {
                                @Override
                                protected List<Integer>
                                getOperandList( int operandCount ) {
                                    return ImmutableList.of( 0, 2 );
                                }
                            } ) );

    private static final PolyReturnTypeInference RETURN_TYPE =
            ReturnTypes.cascade( ReturnTypes.ARG0, ( binding, type ) -> {
                // Result is NOT NULL if NOT NULL default value is provided
                PolyTypeTransform transform;
                if ( binding.getOperandCount() < 3 ) {
                    transform = PolyTypeTransforms.FORCE_NULLABLE;
                } else {
                    AlgDataType defValueType = binding.getOperandType( 2 );
                    transform = defValueType.isNullable()
                            ? PolyTypeTransforms.FORCE_NULLABLE
                            : PolyTypeTransforms.TO_NOT_NULLABLE;
                }
                return transform.transformType( binding, type );
            } );


    public SqlLeadLagAggFunction( Kind kind ) {
        super(
                kind.name(),
                null,
                kind,
                RETURN_TYPE,
                null,
                OPERAND_TYPES,
                FunctionCategory.NUMERIC,
                false,
                true,
                Optionality.FORBIDDEN );
        Preconditions.checkArgument( kind == Kind.LEAD || kind == Kind.LAG );
    }


    @Override
    public boolean allowsFraming() {
        return false;
    }

}

