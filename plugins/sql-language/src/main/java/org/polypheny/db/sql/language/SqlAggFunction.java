/*
 * Copyright 2019-2024 The Polypheny Project
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


import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.fun.SplittableAggFunction;
import org.polypheny.db.plan.Context;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorScope;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.PolyOperandTypeInference;
import org.polypheny.db.type.inference.PolyReturnTypeInference;
import org.polypheny.db.util.Optionality;


/**
 * Abstract base class for the definition of an aggregate function: an operator which aggregates sets of values into a result.
 */
public abstract class SqlAggFunction extends SqlFunction implements Context, AggFunction {

    private final boolean requiresOrder;
    private final boolean requiresOver;
    private final Optionality requiresGroupOrder;


    /**
     * Creates a built-in or user-defined SqlAggFunction or window function.
     *
     * A user-defined function will have a value for {@code sqlIdentifier}; for a built-in function it will be null.
     */
    protected SqlAggFunction(
            String name,
            SqlIdentifier sqlIdentifier,
            Kind kind,
            PolyReturnTypeInference returnTypeInference,
            PolyOperandTypeInference operandTypeInference,
            PolyOperandTypeChecker operandTypeChecker,
            FunctionCategory funcType,
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
    @Override
    public boolean allowsFilter() {
        return true;
    }


    @Override
    public @NotNull <T> Optional<T> unwrap( Class<T> clazz ) {
        if ( clazz == SplittableAggFunction.class ) {
            return Optional.of( clazz.cast( SplittableAggFunction.SelfSplitter.INSTANCE ) );
        }
        return Context.super.unwrap( clazz );
    }

}

