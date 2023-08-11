/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.adapter.ethereum;

import static org.web3j.abi.Utils.convert;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Address;
import org.web3j.abi.datatypes.Event;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.generated.Uint256;

// TODO: extend EventData with Data
public class EventData {

    @Getter
    private String originalKey;
    private String lowercaseKey;
    @Getter
    private Event event;
    @Getter
    private JSONArray abiInputs;


    public EventData( String originalKey, JSONArray abiInputs ) {
        this.originalKey = originalKey;
        this.lowercaseKey = originalKey.toLowerCase();
        this.abiInputs = abiInputs;
        List<TypeReference<?>> typeReferences = createTypeReferences( abiInputs );
        this.event = new Event( originalKey, typeReferences ); // create event based on event name (original key and inputs)
    }

    private static List<TypeReference<?>> createTypeReferences( JSONArray abiInputs ) {
        List<TypeReference<?>> typeReferences = new ArrayList<>();
        for ( int i = 0; i < abiInputs.length(); i++ ) {
            JSONObject inputObject = abiInputs.getJSONObject( i );
            String type = inputObject.getString( "type" );
            boolean indexed = inputObject.getBoolean( "indexed" );
            if ( type.equals( "address" ) ) {
                typeReferences.add( indexed ? new TypeReference<Address>( indexed ) {
                } : new TypeReference<Address>( false ) {
                } );
                // typeReferences.add( new TypeReference<Address>( indexed ) );
            } else if ( type.equals( "uint256" ) ) {
                typeReferences.add( indexed ? new TypeReference<Uint256>( true ) {
                } : new TypeReference<Uint256>( false ) {
                } );
            }
        }
        return typeReferences;
    }

}
