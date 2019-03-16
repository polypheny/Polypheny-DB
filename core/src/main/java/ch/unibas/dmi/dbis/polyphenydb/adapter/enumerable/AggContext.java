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


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlAggFunction;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import java.lang.reflect.Type;
import java.util.List;


/**
 * Information on the aggregate calculation context. {@link AggAddContext} provides basic static information on types of arguments and the return value of the aggregate being implemented.
 */
public interface AggContext {

    /**
     * Returns the aggregation being implemented.
     *
     * @return aggregation being implemented.
     */
    SqlAggFunction aggregation();

    /**
     * Returns the return type of the aggregate as {@link ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType}.
     * This can be helpful to test {@link ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType#isNullable()}.
     *
     * @return return type of the aggregate
     */
    RelDataType returnRelType();

    /**
     * Returns the return type of the aggregate as {@link java.lang.reflect.Type}.
     *
     * @return return type of the aggregate as {@link java.lang.reflect.Type}
     */
    Type returnType();

    /**
     * Returns the parameter types of the aggregate as {@link ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType}.
     * This can be helpful to test {@link ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType#isNullable()}.
     *
     * @return Parameter types of the aggregate
     */
    List<? extends RelDataType> parameterRelTypes();

    /**
     * Returns the parameter types of the aggregate as {@link java.lang.reflect.Type}.
     *
     * @return Parameter types of the aggregate
     */
    List<? extends Type> parameterTypes();

    /**
     * Returns the ordinals of the input fields that make up the key.
     */
    List<Integer> keyOrdinals();

    /**
     * Returns the types of the group key as {@link ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType}.
     */
    List<? extends RelDataType> keyRelTypes();

    /**
     * Returns the types of the group key as {@link java.lang.reflect.Type}.
     */
    List<? extends Type> keyTypes();

    /**
     * Returns the grouping sets we are aggregating on.
     */
    List<ImmutableBitSet> groupSets();
}

