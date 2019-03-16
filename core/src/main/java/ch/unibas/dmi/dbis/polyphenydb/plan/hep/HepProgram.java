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

package ch.unibas.dmi.dbis.polyphenydb.plan.hep;


import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * HepProgram specifies the order in which rules should be attempted by {@link HepPlanner}. Use {@link HepProgramBuilder} to create a new instance of HepProgram.
 *
 * Note that the structure of a program is immutable, but the planner uses it as read/write during planning, so a program can only be in use by a single planner at a time.
 */
public class HepProgram {

    /**
     * Symbolic constant for matching until no more matches occur.
     */
    public static final int MATCH_UNTIL_FIXPOINT = Integer.MAX_VALUE;


    final ImmutableList<HepInstruction> instructions;

    int matchLimit;

    HepMatchOrder matchOrder;

    HepInstruction.EndGroup group;


    /**
     * Creates a new empty HepProgram. The program has an initial match order of {@link HepMatchOrder#ARBITRARY}, and an initial match limit of {@link #MATCH_UNTIL_FIXPOINT}.
     */
    HepProgram( List<HepInstruction> instructions ) {
        this.instructions = ImmutableList.copyOf( instructions );
    }


    public static HepProgramBuilder builder() {
        return new HepProgramBuilder();
    }


    void initialize( boolean clearCache ) {
        matchLimit = MATCH_UNTIL_FIXPOINT;
        matchOrder = HepMatchOrder.DEPTH_FIRST;
        group = null;

        for ( HepInstruction instruction : instructions ) {
            instruction.initialize( clearCache );
        }
    }
}

