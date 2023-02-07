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


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * SQL function that computes keys by which rows can be partitioned and aggregated.
 *
 * Grouped window functions always occur in the GROUP BY clause. They often have auxiliary functions that access information about the group. For example, {@code HOP} is a group function, and its auxiliary functions are
 * {@code HOP_START} and {@code HOP_END}. Here they are used in a streaming query:
 *
 * <blockquote><pre>
 * SELECT STREAM HOP_START(rowtime, INTERVAL '1' HOUR),
 *   HOP_END(rowtime, INTERVAL '1' HOUR),
 *   MIN(unitPrice)
 * FROM Orders
 * GROUP BY HOP(rowtime, INTERVAL '1' HOUR), productId
 * </pre></blockquote>
 */
public class SqlGroupedWindowFunction extends SqlFunction {

    /**
     * The grouped function, if this an auxiliary function; null otherwise.
     */
    public final SqlGroupedWindowFunction groupFunction;


    /**
     * Creates a SqlGroupedWindowFunction.
     *
     * @param name Function name
     * @param kind Kind
     * @param groupFunction Group function, if this is an auxiliary; null, if this is a group function
     * @param returnTypeInference Strategy to use for return type inference
     * @param operandTypeInference Strategy to use for parameter type inference
     * @param operandTypeChecker Strategy to use for parameter type checking
     * @param category Categorization for function
     */
    public SqlGroupedWindowFunction(
            String name,
            Kind kind,
            SqlGroupedWindowFunction groupFunction,
            PolyReturnTypeInference returnTypeInference,
            PolyOperandTypeInference operandTypeInference,
            PolyOperandTypeChecker operandTypeChecker,
            FunctionCategory category ) {
        super( name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, category );
        this.groupFunction = groupFunction;
        Preconditions.checkArgument( groupFunction == null || groupFunction.groupFunction == null );
    }


    /**
     * Creates an auxiliary function from this grouped window function.
     *
     * @param kind Kind; also determines function name
     */
    public SqlGroupedWindowFunction auxiliary( Kind kind ) {
        return auxiliary( kind.name(), kind );
    }


    /**
     * Creates an auxiliary function from this grouped window function.
     *
     * @param name Function name
     * @param kind Kind
     */
    public SqlGroupedWindowFunction auxiliary( String name, Kind kind ) {
        return new SqlGroupedWindowFunction( name, kind, this, ReturnTypes.ARG0, null, getOperandTypeChecker(), FunctionCategory.SYSTEM );
    }


    /**
     * Returns a list of this grouped window function's auxiliary functions.
     */
    public List<SqlGroupedWindowFunction> getAuxiliaryFunctions() {
        return ImmutableList.of();
    }


    @Override
    public boolean isGroup() {
        // Auxiliary functions are not group functions
        return groupFunction == null;
    }


    @Override
    public boolean isGroupAuxiliary() {
        return groupFunction != null;
    }


    @Override
    public Monotonicity getMonotonicity( OperatorBinding call ) {
        // Monotonic iff its first argument is, but not strict.
        //
        // Note: This strategy happens to works for all current group functions (HOP, TUMBLE, SESSION). When there are exceptions to this rule, we'll make the method abstract.
        return ((SqlOperatorBinding) call).getOperandMonotonicity( 0 ).unstrict();
    }

}
