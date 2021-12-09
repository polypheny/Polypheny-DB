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

package org.polypheny.db.algebra;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Context from which a relational expression can initialize itself, reading from a serialized form of the relational expression.
 */
public interface AlgInput {

    AlgOptCluster getCluster();

    AlgTraitSet getTraitSet();

    AlgOptTable getTable( String table );

    /**
     * Returns the input relational expression. Throws if there is not precisely one input.
     */
    AlgNode getInput();

    List<AlgNode> getInputs();

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

    AlgDataType getRowType( String tag );

    AlgDataType getRowType( String expressionsTag, String fieldsTag );

    AlgCollation getCollation();

    AlgDistribution getDistribution();

    ImmutableList<ImmutableList<RexLiteral>> getTuples( String tag );

    boolean getBoolean( String tag, boolean default_ );

}

