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
import org.polypheny.db.algebra.AlgInput;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.fun.UnnestOperator;
import org.polypheny.db.algebra.logical.relational.LogicalRelCorrelate;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.type.MapPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.CoreUtil;


/**
 * Relational expression that unnests its input's columns into a relation.
 *
 * The input may have multiple columns, but each must be a multiset or array. If {@code withOrdinality}, the output contains an extra {@code ORDINALITY} column.
 *
 * Like its inverse operation {@link Collect}, Uncollect is generally invoked in a nested loop, driven by {@link LogicalRelCorrelate} or similar.
 */
public class Uncollect extends SingleAlg {

    public final boolean withOrdinality;


    /**
     * Creates an Uncollect.
     *
     * Use {@link #create} unless you know what you're doing.
     */
    public Uncollect( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, boolean withOrdinality ) {
        super( cluster, traitSet, input );
        this.withOrdinality = withOrdinality;
        assert deriveRowType() != null : "invalid child rowtype";
    }


    /**
     * Creates an Uncollect by parsing serialized output.
     */
    public Uncollect( AlgInput input ) {
        this( input.getCluster(), input.getTraitSet(), input.getInput(), input.getBoolean( "withOrdinality", false ) );
    }


    /**
     * Creates an Uncollect.
     *
     * Each field of the input relational expression must be an array or multiset.
     *
     * @param traitSet Trait set
     * @param input Input relational expression
     * @param withOrdinality Whether output should contain an ORDINALITY column
     */
    public static Uncollect create( AlgTraitSet traitSet, AlgNode input, boolean withOrdinality ) {
        final AlgCluster cluster = input.getCluster();
        return new Uncollect( cluster, traitSet, input, withOrdinality );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).itemIf( "withOrdinality", withOrdinality, withOrdinality );
    }


    @Override
    public final AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return copy( traitSet, sole( inputs ) );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" + input.algCompareString() + "$" + withOrdinality + "&";
    }


    public AlgNode copy( AlgTraitSet traitSet, AlgNode input ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new Uncollect( getCluster(), traitSet, input, withOrdinality );
    }


    @Override
    protected AlgDataType deriveRowType() {
        return deriveUncollectRowType( input, withOrdinality );
    }


    /**
     * Returns the row type returned by applying the 'UNNEST' operation to a relational expression.
     *
     * Each column in the relational expression must be a multiset of structs or an array. The return type is the type of that column, plus an ORDINALITY column if {@code withOrdinality}.
     */
    public static AlgDataType deriveUncollectRowType( AlgNode alg, boolean withOrdinality ) {
        AlgDataType inputType = alg.getTupleType();
        assert inputType.isStruct() : inputType + " is not a struct";
        final List<AlgDataTypeField> fields = inputType.getFields();
        final AlgDataTypeFactory typeFactory = alg.getCluster().getTypeFactory();
        final AlgDataTypeFactory.Builder builder = typeFactory.builder();

        if ( fields.size() == 1 && fields.get( 0 ).getType().getPolyType() == PolyType.ANY ) {
            // Component type is unknown to Uncollect, build a row type with input column name and Any type.
            return builder
                    .add( fields.get( 0 ).getName(), null, PolyType.ANY )
                    .nullable( true )
                    .build();
        }

        for ( AlgDataTypeField field : fields ) {
            if ( field.getType() instanceof MapPolyType ) {
                builder.add( null, UnnestOperator.MAP_KEY_COLUMN_NAME, null, field.getType().unwrap( MapPolyType.class ).orElseThrow().getKeyType() );
                builder.add( null, UnnestOperator.MAP_VALUE_COLUMN_NAME, null, field.getType().unwrap( MapPolyType.class ).orElseThrow().getValueType() );
            } else {
                AlgDataType ret = field.getType().getComponentType();
                assert null != ret;
                if ( ret.isStruct() ) {
                    builder.addAll( ret.getFields() );
                } else {
                    // Element type is not a record. It may be a scalar type, say "INTEGER". Wrap it in a struct type.
                    builder.add( null, CoreUtil.deriveAliasFromOrdinal( field.getIndex() ), null, ret );
                }
            }
        }
        if ( withOrdinality ) {
            builder.add( UnnestOperator.ORDINALITY_COLUMN_NAME, null, PolyType.INTEGER );
        }
        return builder.build();
    }

}
