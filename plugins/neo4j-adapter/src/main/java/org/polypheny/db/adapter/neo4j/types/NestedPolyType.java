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
 */

package org.polypheny.db.adapter.neo4j.types;

import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.impl.Expressible;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PolyType;

public abstract class NestedPolyType implements Expressible {

    public static NestedPolyType from( AlgDataType rowType ) {
        if ( rowType.isStruct() ) {
            return new NestedListType( rowType.getPolyType(), rowType.getFields().stream().map( NestedPolyType::from ).toList() );
        }

        if ( rowType instanceof ArrayType type ) {
            NestedPolyType nested = new NestedSingleType( type.getComponentType().getPolyType() );
            for ( long i = 0; i < type.getDimension(); i++ ) {
                nested = new NestedListType( PolyType.ARRAY, List.of( nested ) );
            }
            return nested;
        }

        return new NestedSingleType( rowType.getPolyType() );

    }


    private static NestedPolyType from( AlgDataTypeField field ) {
        return from( field.getType() );
    }


    public boolean isList() {
        return false;
    }


    public boolean isSingle() {
        return false;
    }


    public NestedListType asList() {
        throw new UnsupportedOperationException();
    }


    public NestedSingleType asSingle() {
        throw new UnsupportedOperationException();
    }


    public abstract PolyType getType();


}
