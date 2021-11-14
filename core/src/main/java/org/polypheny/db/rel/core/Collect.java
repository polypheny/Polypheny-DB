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

package org.polypheny.db.rel.core;


import java.util.List;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelInput;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelWriter;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.type.PolyTypeUtil;


/**
 * A relational expression that collapses multiple rows into one.
 *
 * Rules:
 *
 * <ul>
 * <li>{@code net.sf.farrago.fennel.rel.FarragoMultisetSplitterRule} creates a Collect from a call to {# @link SqlMultisetValueConstructor} or to {# @link SqlMultisetQueryConstructor}.</li>
 * </ul>
 */
public class Collect extends SingleRel {

    protected final String fieldName;


    /**
     * Creates a Collect.
     *
     * @param cluster Cluster
     * @param child Child relational expression
     * @param fieldName Name of the sole output field
     */
    public Collect( RelOptCluster cluster, RelTraitSet traitSet, RelNode child, String fieldName ) {
        super( cluster, traitSet, child );
        this.fieldName = fieldName;
    }


    /**
     * Creates a Collect by parsing serialized output.
     */
    public Collect( RelInput input ) {
        this( input.getCluster(), input.getTraitSet(), input.getInput(), input.getString( "field" ) );
    }


    @Override
    public final RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return copy( traitSet, sole( inputs ) );
    }


    @Override
    public String relCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.relCompareString() + "$" +
                (fieldName != null ? fieldName : "") + "&";
    }


    public RelNode copy( RelTraitSet traitSet, RelNode input ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new Collect( getCluster(), traitSet, input, fieldName );
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
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
    protected RelDataType deriveRowType() {
        return deriveCollectRowType( this, fieldName );
    }


    /**
     * Derives the output type of a collect relational expression.
     *
     * @param rel relational expression
     * @param fieldName name of sole output field
     * @return output type of a collect relational expression
     */
    public static RelDataType deriveCollectRowType( SingleRel rel, String fieldName ) {
        RelDataType childType = rel.getInput().getRowType();
        assert childType.isStruct();
        final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
        RelDataType ret = PolyTypeUtil.createMultisetType( typeFactory, childType, false );
        ret = typeFactory.builder().add( fieldName, null, ret ).build();
        return typeFactory.createTypeWithNullability( ret, false );
    }

}

