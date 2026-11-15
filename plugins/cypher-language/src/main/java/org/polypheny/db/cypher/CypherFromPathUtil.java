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
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.nodes.OperatorBinding;
import org.polypheny.db.rex.RexCallBinding;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PathType;

public class CypherFromPathUtil {

    public static AlgDataType inferReturnType( OperatorBinding opBinding ) {
        return inferReturnType( opBinding.collectOperandTypes(), opBinding );
    }


    public static AlgDataType inferReturnType( List<AlgDataType> operandTypes ) {
        if ( operandTypes.size() < 2 || !(operandTypes.get( 0 ) instanceof PathType pathType) ) {
            throw new GenericRuntimeException( "Could not get element to derive type for extract from path" );
        }
        AlgDataType toExtract = operandTypes.get( 1 );
        String targetName = toExtract.getFieldNames().get( 0 );

        for ( AlgDataTypeField element : pathType.getFields() ) {
            if ( element.getName().equals( targetName ) ) {
                return element.getType();
            }
        }
        throw new GenericRuntimeException( "Could not get element to derive type for extract from path" );
    }


    public static AlgDataType inferReturnType( List<AlgDataType> operandTypes, OperatorBinding opBinding ) {
        if ( operandTypes.size() < 2 || !(operandTypes.get( 0 ) instanceof PathType pathType) || !(opBinding instanceof RexCallBinding callBinding) || !(callBinding.getOperands().get( 1 ) instanceof RexLiteral extract) ) {
            throw new GenericRuntimeException( "Could not get element to derive type for extract from path" );
        }
        String targetName = extract.value.asString().value;

        for ( AlgDataTypeField element : pathType.getFields() ) {
            if ( element.getName().equals( targetName ) ) {
                return element.getType();
            }
        }
        throw new GenericRuntimeException( "Could not get element to derive type for extract from path" );
    }

}
