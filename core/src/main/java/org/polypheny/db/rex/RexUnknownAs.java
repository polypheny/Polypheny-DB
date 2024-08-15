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
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.rex;


import javax.annotation.Nonnull;


/**
 * Policy for whether a simplified expression may instead return another value.
 *
 * In particular, it deals with converting three-valued logic (TRUE, FALSE, UNKNOWN) to two-valued logic (TRUE, FALSE) for callers that treat the UNKNOWN value the same as TRUE or FALSE.
 *
 * Sometimes the three-valued version of the expression is simpler (has a smaller expression tree) than the two-valued version. In these cases, favor simplicity over reduction to two-valued logic.
 *
 * @see RexSimplify
 */
public enum RexUnknownAs {
    /**
     * Policy that indicates that the expression is being used in a context.
     * Where an UNKNOWN value is treated in the same way as FALSE. Therefore, when simplifying the expression, it is acceptable for the simplified expression to evaluate to FALSE in some situations
     * where the original expression would evaluate to UNKNOWN.
     *
     * SQL predicates ({@code WHERE}, {@code ON}, {@code HAVING} and {@code FILTER (WHERE)} clauses, a {@code WHEN} clause of a {@code CASE} expression, and in {@code CHECK} constraints) all treat UNKNOWN as FALSE.
     *
     * If the simplified expression never returns UNKNOWN, the simplifier should make this clear to the caller, if possible, by marking its type as {@code BOOLEAN NOT NULL}.
     */
    FALSE,

    /**
     * Policy that indicates that the expression is being used in a context.
     * Where an UNKNOWN value is treated in the same way as TRUE. Therefore, when simplifying the expression, it is acceptable for the simplified expression to evaluate to TRUE in some situations where the original
     * expression would evaluate to UNKNOWN.
     *
     * This does not occur commonly in SQL. However, it occurs internally during simplification. For example, "{@code WHERE NOT expression}" evaluates "{@code NOT expression}" in a context that treats UNKNOWN as FALSE;
     * it is useful to consider that "{@code expression}" is evaluated in a context that treats UNKNOWN as TRUE.
     *
     * If the simplified expression never returns UNKNOWN, the simplifier should make this clear to the caller, if possible, by marking its type as {@code BOOLEAN NOT NULL}.
     */
    TRUE,

    /**
     * Policy that indicates that the expression is being used in a context.
     * Where an UNKNOWN value is treated as is. This occurs:
     *
     * <ul>
     * <li>In any expression whose type is not {@code BOOLEAN}</li>
     * <li>In {@code BOOLEAN} expressions that are {@code NOT NULL}</li>
     * <li>In {@code BOOLEAN} expressions where {@code UNKNOWN} should be returned as is, for example in a {@code SELECT} clause, or within an expression such as an operand to {@code AND}, {@code OR} or {@code NOT}</li>
     * </ul>
     *
     * If you are unsure, use UNKNOWN. It is the safest option.
     */
    UNKNOWN;


    /**
     * Returns {@link #FALSE} if {@code unknownAsFalse} is true, {@link #UNKNOWN} otherwise.
     */
    public static @Nonnull
    RexUnknownAs falseIf( boolean unknownAsFalse ) {
        return unknownAsFalse ? FALSE : UNKNOWN;
    }


    public boolean toBoolean() {
        return switch ( this ) {
            case FALSE -> false;
            case TRUE -> true;
            case UNKNOWN -> throw new IllegalArgumentException( "unknown" );
        };
    }
}
