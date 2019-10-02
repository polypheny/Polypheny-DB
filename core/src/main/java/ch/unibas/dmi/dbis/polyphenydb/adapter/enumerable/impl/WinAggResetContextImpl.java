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

package ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.impl;


import ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.WinAggResetContext;
import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;


/**
 * Implementation of {@link WinAggResetContext}.
 */
public class WinAggResetContextImpl extends AggResetContextImpl implements WinAggResetContext {

    private final Expression index;
    private final Expression startIndex;
    private final Expression endIndex;
    private final Expression frameRowCount;
    private final Expression partitionRowCount;
    private final Expression hasRows;


    /**
     * Creates window aggregate reset context.
     *
     * @param block code block that will contain the added initialization
     * @param accumulator accumulator variables that store the intermediate aggregate state
     * @param index index of the current row in the partition
     * @param startIndex index of the very first row in partition
     * @param endIndex index of the very last row in partition
     * @param hasRows boolean expression that tells if the partition has rows
     * @param frameRowCount number of rows in the current frame
     * @param partitionRowCount number of rows in the current partition
     */
    public WinAggResetContextImpl( BlockBuilder block, List<Expression> accumulator, Expression index, Expression startIndex, Expression endIndex, Expression hasRows, Expression frameRowCount, Expression partitionRowCount ) {
        super( block, accumulator );
        this.index = index;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.frameRowCount = frameRowCount;
        this.partitionRowCount = partitionRowCount;
        this.hasRows = hasRows;
    }


    @Override
    public Expression index() {
        return index;
    }


    @Override
    public Expression startIndex() {
        return startIndex;
    }


    @Override
    public Expression endIndex() {
        return endIndex;
    }


    @Override
    public Expression hasRows() {
        return hasRows;
    }


    @Override
    public Expression getFrameRowCount() {
        return frameRowCount;
    }


    @Override
    public Expression getPartitionRowCount() {
        return partitionRowCount;
    }
}

