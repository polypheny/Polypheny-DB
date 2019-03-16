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

package ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable;


import org.apache.calcite.linq4j.tree.Expression;


/**
 * Provides information on the current window when computing the result of the aggregation.
 */
public interface WinAggFrameResultContext extends WinAggFrameContext {

    /**
     * Converts absolute index position of the given relative position.
     *
     * @param offset offset of the requested row
     * @param seekType the type of offset (start of window, end of window, etc)
     * @return absolute position of the requested row
     */
    Expression computeIndex( Expression offset, WinAggImplementor.SeekType seekType );

    /**
     * Returns boolean the expression that checks if the given index is in the frame bounds.
     *
     * @param rowIndex index if the row to check
     * @return expression that validates frame bounds for the given index
     */
    Expression rowInFrame( Expression rowIndex );

    /**
     * Returns boolean the expression that checks if the given index is in the partition bounds.
     *
     * @param rowIndex index if the row to check
     * @return expression that validates partition bounds for the given index
     */
    Expression rowInPartition( Expression rowIndex );

    /**
     * Returns row translator for given absolute row position.
     *
     * @param rowIndex absolute index of the row.
     * @return translator for the requested row
     */
    RexToLixTranslator rowTranslator( Expression rowIndex );

    /**
     * Compares two rows given by absolute positions according to the order collation of the current window.
     *
     * @param a absolute index of the first row
     * @param b absolute index of the second row
     * @return result of comparison as as in {@link Comparable#compareTo}
     */
    Expression compareRows( Expression a, Expression b );
}

