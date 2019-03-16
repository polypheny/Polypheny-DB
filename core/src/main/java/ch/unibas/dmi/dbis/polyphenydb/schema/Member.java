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

package ch.unibas.dmi.dbis.polyphenydb.schema;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import java.util.List;
import org.apache.calcite.linq4j.Queryable;


/**
 * A named expression in a schema.
 *
 * <h2>Examples of members</h2>
 *
 * Several kinds of members crop up in real life. They all implement the {@code Member} interface, but tend to be treated differently by the back-end system if not by Polypheny-DB.
 *
 * A member that has zero arguments and a type that is a collection of records is referred to as a <i>relation</i>. In schemas backed by a relational database, tables and views will appear as relations.
 *
 * A member that has one or more arguments and a type that is a collection of records is referred to as a <i>parameterized relation</i>. Some relational databases support these;
 * for example, Oracle calls them "table functions".
 *
 * Members may be also more typical of programming-language functions: they take zero or more arguments, and return a result of arbitrary type.
 *
 * From the above definitions, you can see that a member is a special kind of function. This makes sense, because even though it has no arguments, it is "evaluated" each time it is used in a query.
 */
public interface Member {

    /**
     * The name of this function.
     */
    String getName();

    /**
     * Returns the parameters of this member.
     *
     * @return Parameters; never null
     */
    List<FunctionParameter> getParameters();

    /**
     * Returns the type of this function's result.
     *
     * @return Type of result; never null
     */
    RelDataType getType();

    /**
     * Evaluates this member to yield a result. The result is a {@link org.apache.calcite.linq4j.Queryable}.
     *
     * @param schemaInstance Object that is an instance of the containing {@link Schema}
     * @param arguments List of arguments to the call; must match {@link #getParameters() parameters} in number and type
     * @return An instance of this schema object, as a Queryable
     */
    Queryable evaluate( Object schemaInstance, List<Object> arguments );
}
