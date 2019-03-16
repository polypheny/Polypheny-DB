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

package ch.unibas.dmi.dbis.polyphenydb.plan;


import ch.unibas.dmi.dbis.polyphenydb.rel.RelCollation;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelDistribution;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelReferentialConstraint;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelRoot;
import ch.unibas.dmi.dbis.polyphenydb.rel.logical.LogicalTableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.schema.ColumnStrategy;
import ch.unibas.dmi.dbis.polyphenydb.schema.Wrapper;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;


/**
 * Represents a relational dataset in a {@link RelOptSchema}. It has methods to describe and implement itself.
 */
public interface RelOptTable extends Wrapper {

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
    RelDataType getRowType();

    /**
     * Returns the {@link RelOptSchema} this table belongs to.
     */
    RelOptSchema getRelOptSchema();

    /**
     * Converts this table into a {@link RelNode relational expression}.
     *
     * The {@link ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner planner} calls this method to convert a table into an initial relational expression, generally something abstract, such as a
     * {@link LogicalTableScan}, then optimizes this expression by applying {@link RelOptRule rules} to transform it
     * into more efficient access methods for this table.
     */
    RelNode toRel( ToRelContext context );

    /**
     * Returns a description of the physical ordering (or orderings) of the rows returned from this table.
     *
     * @see RelMetadataQuery#collations(RelNode)
     */
    List<RelCollation> getCollationList();

    /**
     * Returns a description of the physical distribution of the rows in this table.
     *
     * @see RelMetadataQuery#distribution(RelNode)
     */
    RelDistribution getDistribution();

    /**
     * Returns whether the given columns are a key or a superset of a unique key of this table.
     *
     * @param columns Ordinals of key columns
     * @return Whether the given columns are a key or a superset of a key
     */
    boolean isKey( ImmutableBitSet columns );

    /**
     * Returns the referential constraints existing for this table. These constraints are represented over other tables using {@link RelReferentialConstraint} nodes.
     */
    List<RelReferentialConstraint> getReferentialConstraints();

    /**
     * Generates code for this table.
     *
     * @param clazz The desired collection class; for example {@code Queryable}.
     */
    Expression getExpression( Class clazz );

    /**
     * Returns a table with the given extra fields.
     *
     * The extended table includes the fields of this base table plus the extended fields that do not have the same name as a field in the base table.
     */
    RelOptTable extend( List<RelDataTypeField> extendedFields );

    /**
     * Returns a list describing how each column is populated. The list has the same number of entries as there are fields, and is immutable.
     */
    List<ColumnStrategy> getColumnStrategies();

    /**
     * Can expand a view into relational expressions.
     */
    interface ViewExpander {

        /**
         * Returns a relational expression that is to be substituted for an access to a SQL view.
         *
         * @param rowType Row type of the view
         * @param queryString Body of the view
         * @param schemaPath Path of a schema wherein to find referenced tables
         * @param viewPath Path of the view, ending with its name; may be null
         * @return Relational expression
         */
        RelRoot expandView( RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath );
    }


    /**
     * Contains the context needed to convert a a table into a relational expression.
     */
    interface ToRelContext extends ViewExpander {

        RelOptCluster getCluster();
    }
}

