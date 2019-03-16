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
 * Provides information on the current window.
 *
 * All the indexes are ready to be used in {@link WinAggResultContext#arguments(org.apache.calcite.linq4j.tree.Expression)}, {@link WinAggFrameResultContext#rowTranslator(org.apache.calcite.linq4j.tree.Expression)}
 * and similar methods.
 */
public interface WinAggFrameContext {

    /**
     * Returns the index of the current row in the partition. In other words, it is close to ~ROWS BETWEEN CURRENT ROW. Note to use {@link #startIndex()} when you need zero-based row position.
     *
     * @return the index of the very first row in partition
     */
    Expression index();

    /**
     * Returns the index of the very first row in partition.
     *
     * @return index of the very first row in partition
     */
    Expression startIndex();

    /**
     * Returns the index of the very last row in partition.
     *
     * @return index of the very last row in partition
     */
    Expression endIndex();

    /**
     * Returns the boolean expression that tells if the partition has rows. The partition might lack rows in cases like ROWS BETWEEN 1000 PRECEDING AND 900 PRECEDING.
     *
     * @return boolean expression that tells if the partition has rows
     */
    Expression hasRows();

    /**
     * Returns the number of rows in the current frame (subject to framing clause).
     *
     * @return number of rows in the current partition or 0 if the partition is empty
     */
    Expression getFrameRowCount();

    /**
     * Returns the number of rows in the current partition (as determined by PARTITION BY clause).
     *
     * @return number of rows in the current partition or 0 if the partition is empty
     */
    Expression getPartitionRowCount();
}

