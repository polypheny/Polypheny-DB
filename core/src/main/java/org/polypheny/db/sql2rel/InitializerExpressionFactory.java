/*
 * Copyright 2019-2020 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file incorporates code covered by the following terms:
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
 */

package org.polypheny.db.sql2rel;


import java.util.List;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.sql.SqlFunction;


/**
 * InitializerExpressionFactory supplies default values for INSERT, UPDATE, and NEW.
 */
public interface InitializerExpressionFactory {

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

