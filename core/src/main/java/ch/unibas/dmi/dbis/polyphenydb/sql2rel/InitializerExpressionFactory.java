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

package ch.unibas.dmi.dbis.polyphenydb.sql2rel;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.schema.ColumnStrategy;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunction;
import java.util.List;


/**
 * InitializerExpressionFactory supplies default values for INSERT, UPDATE, and NEW.
 */
public interface InitializerExpressionFactory {

    /**
     * Whether a column is always generated. If a column is always generated, then non-generated values cannot be inserted into the column.
     *
     * @see #generationStrategy(RelOptTable, int)
     * @deprecated Use {@code c.generationStrategy(t, i) == VIRTUAL || c.generationStrategy(t, i) == STORED}
     */
    @Deprecated
    // to be removed before 2.0
    boolean isGeneratedAlways( RelOptTable table, int iColumn );

    /**
     * Returns how a column is populated.
     *
     * @param table the table containing the column
     * @param iColumn the 0-based offset of the column in the table
     * @return generation strategy, never null
     * @see RelOptTable#getColumnStrategies()
     */
    ColumnStrategy generationStrategy( RelOptTable table, int iColumn );

    /**
     * Creates an expression which evaluates to the default value for a particular column.
     *
     * @param table the table containing the column
     * @param iColumn the 0-based offset of the column in the table
     * @param context Context for creating the expression
     * @return default value expression
     */
    RexNode newColumnDefaultValue( RelOptTable table, int iColumn, InitializerContext context );

    /**
     * Creates an expression which evaluates to the initializer expression for a particular attribute of a structured type.
     *
     * @param type the structured type
     * @param constructor the constructor invoked to initialize the type
     * @param iAttribute the 0-based offset of the attribute in the type
     * @param constructorArgs arguments passed to the constructor invocation
     * @param context Context for creating the expression
     * @return default value expression
     */
    RexNode newAttributeInitializer( RelDataType type, SqlFunction constructor, int iAttribute, List<RexNode> constructorArgs, InitializerContext context );
}

