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

package ch.unibas.dmi.dbis.polyphenydb.util.mapping;


import java.util.Iterator;


/**
 * A <dfn>Mapping</dfn> is a relationship between a source domain to target domain of integers.
 *
 * This interface represents the most general possible mapping. Depending on the {@link MappingType} of a particular mapping, some of the operations may not be applicable. If you call the method, you will receive a runtime error.
 * For instance:
 *
 * <ul>
 * <li>If a target has more than one source, then the method {@link #getSource(int)} will throw {@link Mappings.TooManyElementsException}.</li>
 * <li>If a source has no targets, then the method {@link #getTarget} will throw {@link Mappings.NoElementException}.</li>
 * </ul>
 */
public interface Mapping extends Mappings.FunctionMapping, Mappings.SourceMapping, Mappings.TargetMapping, Iterable<IntPair> {

    /**
     * Returns an iterator over the elements in this mapping.
     *
     * This method is optional; implementations may throw {@link UnsupportedOperationException}.
     */
    @Override
    Iterator<IntPair> iterator();

    /**
     * Returns the number of sources. Valid sources will be in the range 0 .. sourceCount.
     */
    @Override
    int getSourceCount();

    /**
     * Returns the number of targets. Valid targets will be in the range 0 .. targetCount.
     */
    @Override
    int getTargetCount();

    @Override
    MappingType getMappingType();

    /**
     * Returns whether this mapping is the identity.
     */
    @Override
    boolean isIdentity();

    /**
     * Removes all elements in the mapping.
     */
    void clear();

    /**
     * Returns the number of elements in the mapping.
     */
    @Override
    int size();
}

