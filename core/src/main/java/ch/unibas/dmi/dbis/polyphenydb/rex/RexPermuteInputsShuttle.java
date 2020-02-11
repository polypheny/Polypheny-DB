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

package ch.unibas.dmi.dbis.polyphenydb.rex;


import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings.TargetMapping;
import com.google.common.collect.ImmutableList;
import java.util.List;


/**
 * Shuttle which applies a permutation to its input fields.
 *
 * @see RexPermutationShuttle
 * see RexUtil#apply(TargetMapping, RexNode)
 */
public class RexPermuteInputsShuttle extends RexShuttle {

    private final TargetMapping mapping;
    private final ImmutableList<RelDataTypeField> fields;


    /**
     * Creates a RexPermuteInputsShuttle.
     *
     * The mapping provides at most one target for every source. If a source has no targets and is referenced in the expression, {@link TargetMapping#getTarget(int)}
     * will give an error. Otherwise the mapping gives a unique target.
     *
     * @param mapping Mapping
     * @param inputs Input relational expressions
     */
    public RexPermuteInputsShuttle( TargetMapping mapping, RelNode... inputs ) {
        this( mapping, fields( inputs ) );
    }


    private RexPermuteInputsShuttle( TargetMapping mapping, ImmutableList<RelDataTypeField> fields ) {
        this.mapping = mapping;
        this.fields = fields;
    }


    /**
     * Creates a shuttle with an empty field list. It cannot handle GET calls but otherwise works OK.
     */
    public static RexPermuteInputsShuttle of( TargetMapping mapping ) {
        return new RexPermuteInputsShuttle( mapping, ImmutableList.of() );
    }


    private static ImmutableList<RelDataTypeField> fields( RelNode[] inputs ) {
        final ImmutableList.Builder<RelDataTypeField> fields = ImmutableList.builder();
        for ( RelNode input : inputs ) {
            fields.addAll( input.getRowType().getFieldList() );
        }
        return fields.build();
    }


    @Override
    public RexNode visitInputRef( RexInputRef local ) {
        final int index = local.getIndex();
        int target = mapping.getTarget( index );
        return new RexInputRef( target, local.getType() );
    }


    @Override
    public RexNode visitCall( RexCall call ) {
        if ( call.getOperator() == RexBuilder.GET_OPERATOR ) {
            final String name = (String) ((RexLiteral) call.getOperands().get( 1 )).getValue2();
            final int i = lookup( fields, name );
            if ( i >= 0 ) {
                return RexInputRef.of( i, fields );
            }
        }
        return super.visitCall( call );
    }


    private static int lookup( List<RelDataTypeField> fields, String name ) {
        for ( int i = 0; i < fields.size(); i++ ) {
            final RelDataTypeField field = fields.get( i );
            if ( field.getName().equals( name ) ) {
                return i;
            }
        }
        return -1;
    }
}

