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

package org.polypheny.db.rex;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.util.mapping.Mappings.TargetMapping;


/**
 * Shuttle which applies a permutation to its input fields.
 *
 * @see RexPermutationShuttle
 * see RexUtil#apply(TargetMapping, RexNode)
 */
public class RexPermuteInputsShuttle extends RexShuttle {

    private final TargetMapping mapping;
    private final ImmutableList<AlgDataTypeField> fields;


    /**
     * Creates a RexPermuteInputsShuttle.
     *
     * The mapping provides at most one target for every source. If a source has no targets and is referenced in the expression, {@link TargetMapping#getTarget(int)}
     * will give an error. Otherwise, the mapping gives a unique target.
     *
     * @param mapping Mapping
     * @param inputs Input algebra expressions
     */
    public RexPermuteInputsShuttle( TargetMapping mapping, AlgNode... inputs ) {
        this( mapping, fields( inputs ) );
    }


    private RexPermuteInputsShuttle( TargetMapping mapping, ImmutableList<AlgDataTypeField> fields ) {
        this.mapping = mapping;
        this.fields = fields;
    }


    /**
     * Creates a shuttle with an empty field list. It cannot handle GET calls but otherwise works OK.
     */
    public static RexPermuteInputsShuttle of( TargetMapping mapping ) {
        return new RexPermuteInputsShuttle( mapping, ImmutableList.of() );
    }


    private static ImmutableList<AlgDataTypeField> fields( AlgNode[] inputs ) {
        final ImmutableList.Builder<AlgDataTypeField> fields = ImmutableList.builder();
        for ( AlgNode input : inputs ) {
            fields.addAll( input.getTupleType().getFields() );
        }
        return fields.build();
    }


    @Override
    public RexNode visitIndexRef( RexIndexRef local ) {
        final int index = local.getIndex();
        int target = mapping.getTarget( index );
        return new RexIndexRef( target, local.getType() );
    }


    @Override
    public RexNode visitCall( RexCall call ) {
        if ( call.getOperator().equals( RexBuilder.GET_OPERATOR ) ) {
            final String name = ((RexLiteral) call.getOperands().get( 1 )).getValue().asString().value;
            final int i = lookup( fields, name );
            if ( i >= 0 ) {
                return RexIndexRef.of( i, fields );
            }
        }
        return super.visitCall( call );
    }


    private static int lookup( List<AlgDataTypeField> fields, String name ) {
        for ( int i = 0; i < fields.size(); i++ ) {
            final AlgDataTypeField field = fields.get( i );
            if ( field.getName().equals( name ) ) {
                return i;
            }
        }
        return -1;
    }

}

