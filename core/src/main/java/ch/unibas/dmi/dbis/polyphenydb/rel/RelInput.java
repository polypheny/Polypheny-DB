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

package ch.unibas.dmi.dbis.polyphenydb.rel;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptCluster;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptTable;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.AggregateCall;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexLiteral;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableBitSet;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * Context from which a relational expression can initialize itself, reading from a serialized form of the relational expression.
 */
public interface RelInput {

    RelOptCluster getCluster();

    RelTraitSet getTraitSet();

    RelOptTable getTable( String table );

    /**
     * Returns the input relational expression. Throws if there is not precisely one input.
     */
    RelNode getInput();

    List<RelNode> getInputs();

    /**
     * Returns an expression.
     */
    RexNode getExpression( String tag );

    ImmutableBitSet getBitSet( String tag );

    List<ImmutableBitSet> getBitSetList( String tag );

    List<AggregateCall> getAggregateCalls( String tag );

    Object get( String tag );

    /**
     * Returns a {@code float} value. Throws if wrong type.
     */
    String getString( String tag );

    /**
     * Returns a {@code float} value. Throws if not present or wrong type.
     */
    float getFloat( String tag );

    /**
     * Returns an enum value. Throws if not a valid member.
     */
    <E extends Enum<E>> E getEnum( String tag, Class<E> enumClass );

    List<RexNode> getExpressionList( String tag );

    List<String> getStringList( String tag );

    List<Integer> getIntegerList( String tag );

    List<List<Integer>> getIntegerListList( String tag );

    RelDataType getRowType( String tag );

    RelDataType getRowType( String expressionsTag, String fieldsTag );

    RelCollation getCollation();

    RelDistribution getDistribution();

    ImmutableList<ImmutableList<RexLiteral>> getTuples( String tag );

    boolean getBoolean( String tag, boolean default_ );
}

