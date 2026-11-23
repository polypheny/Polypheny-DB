/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.cypher;

import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.PathType;
import org.polypheny.db.type.PolyType;

public class CypherPathMatchUtil {

    public static AlgDataType inferReturnType( OperatorBinding opBinding ) {
        return inferReturnType( opBinding.collectOperandTypes() );
    }


    public static AlgDataType inferReturnType( List<AlgDataType> operandTypes ) {
        if ( operandTypes.size() < 2 || !(operandTypes.get( 1 ) instanceof PathType) ) {
            throw new GenericRuntimeException( "Could not get element to derive type for path match" );
        }
        return operandTypes.get( 1 );
    }


    public static boolean isRelationshipList( AlgDataType type ) {
        if ( !(type instanceof ArrayType array) ) {
            return false;
        }
        PolyType component = array.getComponentType().getPolyType();
        return array.getCardinality() == 0 || component == PolyType.EDGE || component == PolyType.ANY || component == PolyType.NULL;
    }

}
