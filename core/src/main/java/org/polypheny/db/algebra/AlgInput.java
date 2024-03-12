/*
 * Copyright 2019-2024 The Polypheny Project
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


import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Context from which a relational expression can initialize itself, reading from a serialized form of the relational expression.
 */
public interface AlgInput {

    AlgCluster getCluster();

    AlgTraitSet getTraitSet();

    Entity getEntity( String entity );

    /**
     * Returns the input relational expression. Throws if there is not precisely one input.
     */
    AlgNode getInput();

    List<AlgNode> getInputs();

    ImmutableBitSet getBitSet( String tag );

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

    List<String> getStringList( String tag );

    List<Integer> getIntegerList( String tag );

    AlgDataType getTupleType( String tag );

    AlgCollation getCollation();

    AlgDistribution getDistribution();

    boolean getBoolean( String tag, boolean default_ );

}

