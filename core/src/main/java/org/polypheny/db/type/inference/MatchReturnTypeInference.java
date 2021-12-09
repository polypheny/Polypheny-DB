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
 */

package org.polypheny.db.type.inference;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;


/**
 * Returns the first type that matches a set of given {@link PolyType}s. If no match could be found, null is returned.
 */
public class MatchReturnTypeInference implements PolyReturnTypeInference {

    private final int start;
    private final List<PolyType> typeNames;


    /**
     * Returns the first type of typeName at or after position start (zero based).
     *
     * @see #MatchReturnTypeInference(int, PolyType[])
     */
    public MatchReturnTypeInference( int start, PolyType... typeNames ) {
        this( start, ImmutableList.copyOf( typeNames ) );
    }


    /**
     * Returns the first type matching any type in typeNames at or after position start (zero based).
     */
    public MatchReturnTypeInference( int start, Iterable<PolyType> typeNames ) {
        Preconditions.checkArgument( start >= 0 );
        this.start = start;
        this.typeNames = ImmutableList.copyOf( typeNames );
        Preconditions.checkArgument( !this.typeNames.isEmpty() );
    }


    @Override
    public AlgDataType inferReturnType( OperatorBinding opBinding ) {
        for ( int i = start; i < opBinding.getOperandCount(); i++ ) {
            AlgDataType argType = opBinding.getOperandType( i );
            if ( PolyTypeUtil.isOfSameTypeName( typeNames, argType ) ) {
                return argType;
            }
        }
        return null;
    }

}

