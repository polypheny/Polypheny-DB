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

package org.polypheny.db.functions;

import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;

@Slf4j
public class PolyValueFunctions {


    @SuppressWarnings("unused")
    public static PolyString joinPoint( PolyValue value ) {
        if ( !value.isList() ) {
            throw new GenericRuntimeException( "Value is not a list" );
        }
        return value.asList().stream().map( PolyValue::asString ).reduce( ( a, b ) -> PolyString.of( a + "." + b ) ).orElseThrow();
    }


    public static Function<PolyValue, PolyValue> pickWrapper( String functionName ) {
        switch ( functionName ) {
            case "joinPoint":
                return PolyValueFunctions::joinPoint;
            default:
                throw new GenericRuntimeException( "Unknown function: " + functionName );
        }
    }

}
