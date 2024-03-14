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

package org.polypheny.db.algebra.core;


import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.type.PolyTypeUtil;


/**
 * A relational expression that collapses multiple rows into one.
 *
 * Rules:
 *
 * <ul>
 * <li>{@code net.sf.farrago.fennel.alg.FarragoMultisetSplitterRule} creates a Collect from a call to {# @link SqlMultisetValueConstructor} or to {# @link SqlMultisetQueryConstructor}.</li>
 * </ul>
 */
public class Collect extends SingleAlg {

    protected final String fieldName;


    /**
     * Creates a Collect.
     *
     * @param cluster Cluster
     * @param child Child relational expression
     * @param fieldName Name of the sole output field
     */
    public Collect( AlgCluster cluster, AlgTraitSet traitSet, AlgNode child, String fieldName ) {
        super( cluster, traitSet, child );
        this.fieldName = fieldName;
    }


    @Override
    public final AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return copy( traitSet, sole( inputs ) );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                (fieldName != null ? fieldName : "") + "&";
    }


    public AlgNode copy( AlgTraitSet traitSet, AlgNode input ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new Collect( getCluster(), traitSet, input, fieldName );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "field", fieldName );
    }


    /**
     * Returns the name of the sole output field.
     *
     * @return name of the sole output field
     */
    public String getFieldName() {
        return fieldName;
    }


    @Override
    protected AlgDataType deriveRowType() {
        return deriveCollectRowType( this, fieldName );
    }


    /**
     * Derives the output type of a collect relational expression.
     *
     * @param alg relational expression
     * @param fieldName name of sole output field
     * @return output type of a collect relational expression
     */
    public static AlgDataType deriveCollectRowType( SingleAlg alg, String fieldName ) {
        AlgDataType childType = alg.getInput().getTupleType();
        assert childType.isStruct();
        final AlgDataTypeFactory typeFactory = alg.getCluster().getTypeFactory();
        AlgDataType ret = PolyTypeUtil.createMultisetType( typeFactory, childType, false );
        ret = typeFactory.builder().add( null, fieldName, null, ret ).build();
        return typeFactory.createTypeWithNullability( ret, false );
    }

}

