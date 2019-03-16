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
import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;


/**
 * Information for a call to {@link AggImplementor#implementAdd(AggContext, AggAddContext)}.
 *
 * Typically, the aggregation implementation will use {@link #arguments()} or {@link #rexArguments()} to update aggregate value.
 */
public interface AggAddContext extends AggResultContext {

    /**
     * Returns {@link RexNode} representation of arguments. This can be useful for manual translation of required arguments with different {@link NullPolicy}.
     *
     * @return {@link RexNode} representation of arguments
     */
    List<RexNode> rexArguments();

    /**
     * Returns {@link RexNode} representation of the filter, or null.
     */
    RexNode rexFilterArgument();

    /**
     * Returns Linq4j form of arguments. The resulting value is equivalent to {@code rowTranslator().translateList(rexArguments())}. This is handy if you need just operate on argument.
     *
     * @return Linq4j form of arguments.
     */
    List<Expression> arguments();

    /**
     * Returns a {@link RexToLixTranslator} suitable to transform the arguments.
     *
     * @return {@link RexToLixTranslator} suitable to transform the arguments
     */
    RexToLixTranslator rowTranslator();
}

