/*
 * Copyright 2019-2022 The Polypheny Project
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
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgDistribution;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgReferentialConstraint;
import org.polypheny.db.algebra.logical.relational.LogicalScan;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.schema.ColumnStrategy;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.Wrapper;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Represents a relational dataset in a {@link AlgOptSchema}. It has methods to describe and implement itself.
 */
public interface AlgOptTable extends Wrapper {

    /**
     * Obtains an identifier for this table. The identifier must be unique with respect to the Connection producing this table.
     *
     * @return qualified name
     */
    List<String> getQualifiedName();

    /**
     * Returns an estimate of the number of rows in the table.
     */
    double getRowCount();

    /**
     * Describes the type of rows returned by this table.
     */
    AlgDataType getRowType();

    /**
     * Returns the {@link AlgOptSchema} this table belongs to.
     */
    AlgOptSchema getRelOptSchema();

    /**
     * Converts this table into a {@link AlgNode relational expression}.
     *
     * The {@link AlgOptPlanner planner} calls this method to convert a table into an initial
     * relational expression, generally something abstract, such as a {@link LogicalScan}, then optimizes this
     * expression by applying {@link AlgOptRule rules} to transform it into more efficient access methods for this table.
     */
    AlgNode toAlg( ToAlgContext context );

    /**
     * Returns a description of the physical ordering (or orderings) of the rows returned from this table.
     *
     * @see AlgMetadataQuery#collations(AlgNode)
     */
    List<AlgCollation> getCollationList();

    /**
     * Returns a description of the physical distribution of the rows in this table.
     *
     * @see AlgMetadataQuery#distribution(AlgNode)
     */
    AlgDistribution getDistribution();

    /**
     * Returns whether the given columns are a key or a superset of a unique key of this table.
     *
     * @param columns Ordinals of key columns
     * @return Whether the given columns are a key or a superset of a key
     */
    boolean isKey( ImmutableBitSet columns );

    /**
     * Returns the referential constraints existing for this table. These constraints are represented over other tables
     * using {@link AlgReferentialConstraint} nodes.
     */
    List<AlgReferentialConstraint> getReferentialConstraints();

    /**
     * Generates code for this table.
     *
     * @param clazz The desired collection class; for example {@code Queryable}.
     */
    Expression getExpression( Class clazz );

    /**
     * Returns a table with the given extra fields.
     *
     * The extended table includes the fields of this base table plus the extended fields that do not have the same name as
     * a field in the base table.
     */
    AlgOptTable extend( List<AlgDataTypeField> extendedFields );

    /**
     * Returns a list describing how each column is populated. The list has the same number of entries as there are fields,
     * and is immutable.
     */
    List<ColumnStrategy> getColumnStrategies();


    default Table getTable() {
        return null;
    }


    /**
     * Contains the context needed to convert a table into a relational expression.
     */
    interface ToAlgContext {

        AlgOptCluster getCluster();

    }

}

