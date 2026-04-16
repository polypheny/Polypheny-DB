/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.type;

import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataType;

/**
 * Marker for Arrays of the form (dim, card) = (1, n) and entries in {@link ElementType}.
 */
public class VectorType extends ArrayType {

    public enum ElementType {
        FLOAT, DOUBLE, INTEGER, BIT
    }

    @Getter
    private final ElementType vectorElementType;

    public VectorType( AlgDataType elementType, boolean isNullable, long
            dimension, ElementType vectorElementType ) {
        super( elementType, isNullable, dimension, 1 );
        this.vectorElementType = vectorElementType;
    }

    public long getVectorDimension() {
        return getCardinality();
    }

}
