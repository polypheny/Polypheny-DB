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


import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelInput;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelWriter;
import org.polypheny.db.rel.SingleRel;
import org.polypheny.db.rel.logical.LogicalCorrelate;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.sql.SqlUnnestOperator;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.type.MapSqlType;
import org.polypheny.db.sql.type.SqlTypeName;
import java.util.List;


/**
 * Relational expression that unnests its input's columns into a relation.
 *
 * The input may have multiple columns, but each must be a multiset or array. If {@code withOrdinality}, the output contains an extra {@code ORDINALITY} column.
 *
 * Like its inverse operation {@link Collect}, Uncollect is generally invoked in a nested loop, driven by {@link LogicalCorrelate} or similar.
 */
public class Uncollect extends SingleRel {

    public final boolean withOrdinality;


    /**
     * Creates an Uncollect.
     *
     * Use {@link #create} unless you know what you're doing.
     */
    public Uncollect( RelOptCluster cluster, RelTraitSet traitSet, RelNode input, boolean withOrdinality ) {
        super( cluster, traitSet, input );
        this.withOrdinality = withOrdinality;
        assert deriveRowType() != null : "invalid child rowtype";
    }


    /**
     * Creates an Uncollect by parsing serialized output.
     */
    public Uncollect( RelInput input ) {
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
    public static Uncollect create( RelTraitSet traitSet, RelNode input, boolean withOrdinality ) {
        final RelOptCluster cluster = input.getCluster();
        return new Uncollect( cluster, traitSet, input, withOrdinality );
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        return super.explainTerms( pw ).itemIf( "withOrdinality", withOrdinality, withOrdinality );
    }


    @Override
    public final RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
        return copy( traitSet, sole( inputs ) );
    }


    public RelNode copy( RelTraitSet traitSet, RelNode input ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        return new Uncollect( getCluster(), traitSet, input, withOrdinality );
    }


    @Override
    protected RelDataType deriveRowType() {
        return deriveUncollectRowType( input, withOrdinality );
    }


    /**
     * Returns the row type returned by applying the 'UNNEST' operation to a relational expression.
     *
     * Each column in the relational expression must be a multiset of structs or an array. The return type is the type of that column, plus an ORDINALITY column if {@code withOrdinality}.
     */
    public static RelDataType deriveUncollectRowType( RelNode rel, boolean withOrdinality ) {
        RelDataType inputType = rel.getRowType();
        assert inputType.isStruct() : inputType + " is not a struct";
        final List<RelDataTypeField> fields = inputType.getFieldList();
        final RelDataTypeFactory typeFactory = rel.getCluster().getTypeFactory();
        final RelDataTypeFactory.Builder builder = typeFactory.builder();

        if ( fields.size() == 1 && fields.get( 0 ).getType().getSqlTypeName() == SqlTypeName.ANY ) {
            // Component type is unknown to Uncollect, build a row type with input column name and Any type.
            return builder
                    .add( fields.get( 0 ).getName(), null, SqlTypeName.ANY )
                    .nullable( true )
                    .build();
        }

        for ( RelDataTypeField field : fields ) {
            if ( field.getType() instanceof MapSqlType ) {
                builder.add( SqlUnnestOperator.MAP_KEY_COLUMN_NAME, null, field.getType().getKeyType() );
                builder.add( SqlUnnestOperator.MAP_VALUE_COLUMN_NAME, null, field.getType().getValueType() );
            } else {
                RelDataType ret = field.getType().getComponentType();
                assert null != ret;
                if ( ret.isStruct() ) {
                    builder.addAll( ret.getFieldList() );
                } else {
                    // Element type is not a record. It may be a scalar type, say "INTEGER". Wrap it in a struct type.
                    builder.add( SqlUtil.deriveAliasFromOrdinal( field.getIndex() ), null, ret );
                }
            }
        }
        if ( withOrdinality ) {
            builder.add( SqlUnnestOperator.ORDINALITY_COLUMN_NAME, null, SqlTypeName.INTEGER );
        }
        return builder.build();
    }
}
