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

package ch.unibas.dmi.dbis.polyphenydb.sql;


import ch.unibas.dmi.dbis.polyphenydb.plan.Context;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeChecker;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlOperandTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlReturnTypeInference;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorScope;
import ch.unibas.dmi.dbis.polyphenydb.util.Optionality;
import java.util.Objects;
import javax.annotation.Nonnull;


/**
 * Abstract base class for the definition of an aggregate function: an operator which aggregates sets of values into a result.
 */
public abstract class SqlAggFunction extends SqlFunction implements Context {

    private final boolean requiresOrder;
    private final boolean requiresOver;
    private final Optionality requiresGroupOrder;


    /**
     * Creates a built-in SqlAggFunction.
     */
    @Deprecated // to be removed before 2.0
    protected SqlAggFunction(
            String name,
            SqlKind kind,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory funcType ) {
        // We leave sqlIdentifier as null to indicate that this is a builtin.
        this(
                name,
                null,
                kind,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                funcType,
                false,
                false,
                Optionality.FORBIDDEN );
    }


    /**
     * Creates a user-defined SqlAggFunction.
     */
    @Deprecated // to be removed before 2.0
    protected SqlAggFunction(
            String name,
            SqlIdentifier sqlIdentifier,
            SqlKind kind,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory funcType ) {
        this(
                name,
                sqlIdentifier,
                kind,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                funcType,
                false,
                false,
                Optionality.FORBIDDEN );
    }


    @Deprecated // to be removed before 2.0
    protected SqlAggFunction(
            String name,
            SqlIdentifier sqlIdentifier,
            SqlKind kind,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory funcType,
            boolean requiresOrder,
            boolean requiresOver ) {
        this(
                name,
                sqlIdentifier,
                kind,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                funcType,
                requiresOrder,
                requiresOver,
                Optionality.FORBIDDEN );
    }


    /**
     * Creates a built-in or user-defined SqlAggFunction or window function.
     *
     * A user-defined function will have a value for {@code sqlIdentifier}; for a built-in function it will be null.
     */
    protected SqlAggFunction(
            String name,
            SqlIdentifier sqlIdentifier,
            SqlKind kind,
            SqlReturnTypeInference returnTypeInference,
            SqlOperandTypeInference operandTypeInference,
            SqlOperandTypeChecker operandTypeChecker,
            SqlFunctionCategory funcType,
            boolean requiresOrder,
            boolean requiresOver,
            Optionality requiresGroupOrder ) {
        super(
                name,
                sqlIdentifier,
                kind,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                null,
                funcType );
        this.requiresOrder = requiresOrder;
        this.requiresOver = requiresOver;
        this.requiresGroupOrder = Objects.requireNonNull( requiresGroupOrder );
    }


    @Override
    public <T> T unwrap( Class<T> clazz ) {
        return clazz.isInstance( this ) ? clazz.cast( this ) : null;
    }


    @Override
    public boolean isAggregator() {
        return true;
    }


    @Override
    public boolean isQuantifierAllowed() {
        return true;
    }


    @Override
    public void validateCall( SqlCall call, SqlValidator validator, SqlValidatorScope scope, SqlValidatorScope operandScope ) {
        super.validateCall( call, validator, scope, operandScope );
        validator.validateAggregateParams( call, null, null, scope );
    }


    @Override
    public final boolean requiresOrder() {
        return requiresOrder;
    }


    /**
     * Returns whether this aggregate function must, may, or must not contain a {@code WITHIN GROUP (ORDER ...)} clause.
     *
     * Cases:
     *
     * <ul>
     * <li>If {@link Optionality#MANDATORY}, then {@code AGG(x) WITHIN GROUP (ORDER BY 1)} is valid, and {@code AGG(x)} is invalid.</li>
     * <li>If {@link Optionality#OPTIONAL}, then {@code AGG(x) WITHIN GROUP (ORDER BY 1)} and {@code AGG(x)} are both valid.</li>
     * <li>If {@link Optionality#IGNORED}, then {@code AGG(x)} is valid, and {@code AGG(x) WITHIN GROUP (ORDER BY 1)} is valid but is treated the same as {@code AGG(x)}.</li>
     * <li>If {@link Optionality#FORBIDDEN}, then {@code AGG(x) WITHIN GROUP (ORDER BY 1)} is invalid, and {@code AGG(x)} is valid.</li>
     * </ul>
     */
    public @Nonnull
    Optionality requiresGroupOrder() {
        return requiresGroupOrder;
    }


    @Override
    public final boolean requiresOver() {
        return requiresOver;
    }


    /**
     * Whether this aggregate function allows a {@code FILTER (WHERE ...)} clause.
     */
    public boolean allowsFilter() {
        return true;
    }
}

