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

package org.polypheny.db.algebra.constant;


/**
 * Enumeration of types of monotonicity.
 */
public enum Monotonicity {
    STRICTLY_INCREASING,
    INCREASING,
    STRICTLY_DECREASING,
    DECREASING,
    CONSTANT,
    /**
     * Catch-all value for expressions that have some monotonic properties. Maybe it isn't known whether the expression is increasing or decreasing;
     * or maybe the value is neither increasing nor decreasing but the value never repeats.
     */
    MONOTONIC,
    NOT_MONOTONIC;


    /**
     * If this is a strict monotonicity (StrictlyIncreasing, StrictlyDecreasing) returns the non-strict equivalent (Increasing, Decreasing).
     *
     * @return non-strict equivalent monotonicity
     */
    public Monotonicity unstrict() {
        switch ( this ) {
            case STRICTLY_INCREASING:
                return INCREASING;
            case STRICTLY_DECREASING:
                return DECREASING;
            default:
                return this;
        }
    }


    /**
     * Returns the reverse monotonicity.
     *
     * @return reverse monotonicity
     */
    public Monotonicity reverse() {
        switch ( this ) {
            case STRICTLY_INCREASING:
                return STRICTLY_DECREASING;
            case INCREASING:
                return DECREASING;
            case STRICTLY_DECREASING:
                return STRICTLY_INCREASING;
            case DECREASING:
                return INCREASING;
            default:
                return this;
        }
    }


    /**
     * Returns whether values of this monotonicity may ever repeat after moving to another value: true for {@link #NOT_MONOTONIC} and {@link #CONSTANT}, false otherwise.
     *
     * If a column is known not to repeat, a sort on that column can make progress before all of the input has been seen.
     *
     * @return whether values repeat
     */
    public boolean mayRepeat() {
        switch ( this ) {
            case NOT_MONOTONIC:
            case CONSTANT:
                return true;
            default:
                return false;
        }
    }
}
