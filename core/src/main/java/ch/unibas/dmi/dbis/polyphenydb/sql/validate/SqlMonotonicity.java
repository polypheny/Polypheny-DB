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

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


/**
 * Enumeration of types of monotonicity.
 */
public enum SqlMonotonicity {
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
    public SqlMonotonicity unstrict() {
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
    public SqlMonotonicity reverse() {
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
     * Whether values of this monotonicity are decreasing. That is, if a value at a given point in a sequence is X, no point later in the sequence will have a value greater than X.
     *
     * @return whether values are decreasing
     */
    public boolean isDecreasing() {
        switch ( this ) {
            case STRICTLY_DECREASING:
            case DECREASING:
                return true;
            default:
                return false;
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

