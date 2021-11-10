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

package org.polypheny.db.core;

import lombok.Getter;
import org.polypheny.db.util.Litmus;

public abstract class Operator {

    /**
     * See {@link Kind}. It's possible to have a name that doesn't match the kind
     */
    @Getter
    public final Kind kind;
    /**
     * The name of the operator/function. Ex. "OVERLAY" or "TRIM"
     */
    @Getter
    protected final String name;


    public Operator( String name, Kind kind ) {
        this.name = name;
        this.kind = kind;
    }


    /**
     * Returns whether the given operands are valid. If not valid and {@code fail}, throws an assertion error.
     *
     * Similar to {@link #checkOperandCount}, but some operators may have different valid operands in {@link SqlNode} and {@code RexNode} formats (some examples are CAST and AND),
     * and this method throws internal errors, not user errors.
     */
    public boolean validRexOperands( int count, Litmus litmus ) {
        return true;
    }

}
