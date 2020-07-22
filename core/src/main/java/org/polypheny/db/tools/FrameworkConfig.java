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

package org.polypheny.db.tools;


import com.google.common.collect.ImmutableList;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.RelOptCostFactory;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTraitDef;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rex.RexExecutor;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.sql.SqlOperatorTable;
import org.polypheny.db.sql.parser.SqlParser.SqlParserConfig;
import org.polypheny.db.sql2rel.SqlRexConvertletTable;
import org.polypheny.db.sql2rel.SqlToRelConverter;


/**
 * Interface that describes how to configure planning sessions generated using the Frameworks tools.
 *
 * @see Frameworks#newConfigBuilder()
 */
public interface FrameworkConfig {

    /**
     * The configuration of SQL parser.
     */
    SqlParserConfig getParserConfig();

    /**
     * The configuration of {@link SqlToRelConverter}.
     */
    SqlToRelConverter.Config getSqlToRelConverterConfig();

    /**
     * Returns the default schema that should be checked before looking at the root schema.  Returns null to only consult the root schema.
     */
    SchemaPlus getDefaultSchema();

    /**
     * Returns the executor used to evaluate constant expressions.
     */
    RexExecutor getExecutor();

    /**
     * Returns a list of one or more programs used during the course of query evaluation.
     * <p>
     * The common use case is when there is a single program created using {@link Programs#of(RuleSet)} and
     * {@link org.polypheny.db.tools.Planner#transform} will only be called once.
     * <p>
     * However, consumers may also create programs not based on rule sets, register multiple programs, and do multiple
     * repetitions of {@link Planner#transform} planning cycles using different indices.
     * <p>
     * The order of programs provided here determines the zero-based indices of programs elsewhere in this class.
     */
    ImmutableList<Program> getPrograms();

    /**
     * Returns operator table that should be used to resolve functions and operators during query validation.
     */
    SqlOperatorTable getOperatorTable();

    /**
     * Returns the cost factory that should be used when creating the planner.
     * If null, use the default cost factory for that planner.
     */
    RelOptCostFactory getCostFactory();

    /**
     * Returns a list of trait definitions.
     *
     * If the list is not null, the planner first de-registers any existing {@link RelTraitDef}s, then registers the
     * {@code RelTraitDef}s in this list.
     *
     * The order of {@code RelTraitDef}s in the list matters if the planner is VolcanoPlanner. The planner calls
     * {@link RelTraitDef#convert} in the order of this list. The most important trait comes first in the list,
     * followed by the second most important one, etc.
     */
    ImmutableList<RelTraitDef> getTraitDefs();

    /**
     * Returns the convertlet table that should be used when converting from SQL to row expressions
     */
    SqlRexConvertletTable getConvertletTable();

    /**
     * Returns the PlannerContext that should be made available during planning by calling {@link org.polypheny.db.plan.RelOptPlanner#getContext()}.
     */
    Context getContext();

    /**
     * Returns the type system.
     */
    RelDataTypeSystem getTypeSystem();

    /**
     * Returns a view expander.
     */
    RelOptTable.ViewExpander getViewExpander();

    /**
     * Returns a prepare context.
     */
    org.polypheny.db.jdbc.Context getPrepareContext();
}

