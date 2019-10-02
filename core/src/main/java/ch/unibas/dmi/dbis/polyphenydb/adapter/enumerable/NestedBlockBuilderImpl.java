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


import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.tree.BlockBuilder;


/**
 * Allows to build nested code blocks with tracking of current context and the nullability of particular {@link RexNode} expressions.
 *
 * @see ch.unibas.dmi.dbis.polyphenydb.adapter.enumerable.StrictAggImplementor#implementAdd(AggContext, AggAddContext)
 */
public class NestedBlockBuilderImpl implements NestedBlockBuilder {

    private final List<BlockBuilder> blocks = new ArrayList<>();
    private final List<Map<RexNode, Boolean>> nullables = new ArrayList<>();


    /**
     * Constructs nested block builders starting of a given code block.
     *
     * @param block root code block
     */
    public NestedBlockBuilderImpl( BlockBuilder block ) {
        nestBlock( block );
    }


    /**
     * Starts nested code block. The resulting block can optimize expressions and reuse already calculated values from the parent blocks.
     *
     * @return new code block that can optimize expressions and reuse already calculated values from the parent blocks.
     */
    @Override
    public final BlockBuilder nestBlock() {
        BlockBuilder block = new BlockBuilder( true, currentBlock() );
        nestBlock( block, Collections.emptyMap() );
        return block;
    }


    /**
     * Uses given block as the new code context. The current block will be restored after {@link #exitBlock()} call.
     *
     * @param block new code block
     * @see #exitBlock()
     */
    @Override
    public final void nestBlock( BlockBuilder block ) {
        nestBlock( block, Collections.emptyMap() );
    }


    /**
     * Uses given block as the new code context and the map of nullability. The current block will be restored after {@link #exitBlock()} call.
     *
     * @param block new code block
     * @param nullables map of expression to its nullability state
     * @see #exitBlock()
     */
    @Override
    public final void nestBlock( BlockBuilder block, Map<RexNode, Boolean> nullables ) {
        blocks.add( block );
        Map<RexNode, Boolean> prev = this.nullables.isEmpty()
                ? Collections.emptyMap()
                : this.nullables.get( this.nullables.size() - 1 );
        Map<RexNode, Boolean> next;
        if ( nullables == null || nullables.isEmpty() ) {
            next = prev;
        } else {
            next = new HashMap<>( nullables );
            next.putAll( prev );
            next = Collections.unmodifiableMap( next );
        }
        this.nullables.add( next );
    }


    /**
     * Returns the current code block
     *
     * @return current code block
     */
    @Override
    public final BlockBuilder currentBlock() {
        return blocks.get( blocks.size() - 1 );
    }


    /**
     * Returns the current nullability state of rex nodes. The resulting value is the summary of all the maps in the block hierarchy.
     *
     * @return current nullability state of rex nodes
     */
    @Override
    public final Map<RexNode, Boolean> currentNullables() {
        return nullables.get( nullables.size() - 1 );
    }


    /**
     * Leaves the current code block.
     *
     * @see #nestBlock()
     */
    @Override
    public final void exitBlock() {
        blocks.remove( blocks.size() - 1 );
        nullables.remove( nullables.size() - 1 );
    }
}

