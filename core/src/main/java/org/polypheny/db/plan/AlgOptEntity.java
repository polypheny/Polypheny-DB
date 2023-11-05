/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.plan;


import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.schema.Entity;
import org.polypheny.db.schema.Wrapper;


/**
 * Represents a relational dataset in a {@link AlgOptSchema}. It has methods to describe and implement itself.
 */
public interface AlgOptEntity extends Wrapper {

    /**
     * Returns an estimate of the number of rows in the table.
     */
    double getRowCount();

    /**
     * Describes the type of rows returned by this table.
     */
    AlgDataType getRowType();


    /**
     * Converts this table into a {@link AlgNode relational expression}.
     *
     * The {@link AlgOptPlanner planner} calls this method to convert a table into an initial
     * relational expression, generally something abstract, such as a {@link LogicalRelScan}, then optimizes this
     * expression by applying {@link AlgOptRule rules} to transform it into more efficient access methods for this table.
     */
    AlgNode toAlg( ToAlgContext context, AlgTraitSet traitSet );

    /**
     * Returns a description of the physical distribution of the rows in this table.
     *
     * @see AlgMetadataQuery#distribution(AlgNode)
     */
    AlgDistribution getDistribution();


    /**
     * Generates code for this table.
     *
     * @param clazz The desired collection class; for example {@code Queryable}.
     */
    Expression getExpression( Class<?> clazz );

    /**
     * Returns a list describing how each column is populated. The list has the same number of entries as there are fields,
     * and is immutable.
     */
    List<ColumnStrategy> getColumnStrategies();

    @Deprecated
    default Entity getEntity() {
        return null;
    }


    /**
     * Contains the context needed to convert a table into a relational expression.
     */
    interface ToAlgContext {

        AlgOptCluster getCluster();

    }

}

