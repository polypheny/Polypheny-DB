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

package org.polypheny.db.languages.sql;


import org.polypheny.db.core.OperatorBinding;
import org.polypheny.db.languages.sql.validate.SqlMonotonicity;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.util.NlsString;


/**
 * <code>SqlOperatorBinding</code> represents the binding of an {@link SqlOperator} to actual operands, along with any additional information required to validate those operands if needed.
 */
public abstract class SqlOperatorBinding extends OperatorBinding {


    /**
     * Creates a SqlOperatorBinding.
     *
     * @param typeFactory Type factory
     * @param sqlOperator Operator which is subject of this call
     */
    protected SqlOperatorBinding( RelDataTypeFactory typeFactory, SqlOperator sqlOperator ) {
        super( typeFactory, sqlOperator );
    }


    /**
     * Gets the value of a literal operand.
     *
     * Cases:
     * <ul>
     * <li>If the operand is not a literal, the value is null.</li>
     * <li>If the operand is a string literal, the value will be of type {@link NlsString}.</li>
     * <li>If the operand is a numeric literal, the value will be of type {@link java.math.BigDecimal}.</li>
     * <li>If the operand is an interval qualifier, the value will be of type {@link SqlIntervalQualifier}</li>
     * <li>Otherwise the type is undefined, and the value may be null.</li>
     * </ul>
     *
     * @param ordinal zero-based ordinal of operand of interest
     * @param clazz Desired valued type
     * @return value of operand
     */
    public <T> T getOperandLiteralValue( int ordinal, Class<T> clazz ) {
        throw new UnsupportedOperationException();
    }


    /**
     * Gets the monotonicity of a bound operand.
     *
     * @param ordinal zero-based ordinal of operand of interest
     * @return monotonicity of operand
     */
    public SqlMonotonicity getOperandMonotonicity( int ordinal ) {
        return SqlMonotonicity.NOT_MONOTONIC;
    }


}

