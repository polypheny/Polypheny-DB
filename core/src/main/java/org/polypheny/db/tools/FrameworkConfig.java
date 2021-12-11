/*
 * Copyright 2019-2021 The Polypheny Project
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
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.languages.NodeToAlgConverter;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.languages.RexConvertletTable;
import org.polypheny.db.plan.AlgOptCostFactory;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitDef;
import org.polypheny.db.plan.Context;
import org.polypheny.db.rex.RexExecutor;
import org.polypheny.db.schema.SchemaPlus;


/**
 * Interface that describes how to configure planning sessions generated using the Frameworks tools.
 *
 * @see Frameworks#newConfigBuilder()
 */
public interface FrameworkConfig {

    /**
     * Returns the configuration of SQL parser.
     */
    ParserConfig getParserConfig();

    /**
     * The configuration of {@link NodeToAlgConverter}.
     */
    NodeToAlgConverter.Config getSqlToRelConverterConfig();

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
    OperatorTable getOperatorTable();

    /**
     * Returns the cost factory that should be used when creating the planner.
     * If null, use the default cost factory for that planner.
     */
    AlgOptCostFactory getCostFactory();

    /**
     * Returns a list of trait definitions.
     *
     * If the list is not null, the planner first de-registers any existing {@link AlgTraitDef}s, then registers the
     * {@code RelTraitDef}s in this list.
     *
     * The order of {@code RelTraitDef}s in the list matters if the planner is VolcanoPlanner. The planner calls
     * {@link AlgTraitDef#convert} in the order of this list. The most important trait comes first in the list,
     * followed by the second most important one, etc.
     */
    ImmutableList<AlgTraitDef> getTraitDefs();

    /**
     * Returns the convertlet table that should be used when converting from SQL to row expressions
     */
    RexConvertletTable getConvertletTable();

    /**
     * Returns the PlannerContext that should be made available during planning by calling {@link AlgOptPlanner#getContext()}.
     */
    Context getContext();

    /**
     * Returns the type system.
     */
    AlgDataTypeSystem getTypeSystem();

    /**
     * Returns a prepare context.
     */
    org.polypheny.db.prepare.Context getPrepareContext();

}

