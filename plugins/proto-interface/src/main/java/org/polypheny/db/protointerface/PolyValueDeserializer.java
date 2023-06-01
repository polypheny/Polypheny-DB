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

package org.polypheny.db.protointerface;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.protointerface.proto.ParameterizedStatementOrBuilder;
import org.polypheny.db.protointerface.proto.Value;
import org.polypheny.db.protointerface.proto.ValueMap;
import org.polypheny.db.protointerface.proto.ValueMapBatch;
import org.polypheny.db.type.entity.PolyValue;

public class PolyValueDeserializer {

    public static Map<String, PolyValue> deserializeValueMap( Map<String, Value> protoValueMap ) {
        throw new NotImplementedException( "not yet implemeted" );
    }
        //TODO: implementation

    public static List<Map<String, PolyValue>> deserializeValueMapBatch( ValueMapBatch valueMapBatch ) {
        return valueMapBatch.getValueMapsList().stream()
                .map( ValueMap::getValueMapMap )
                .map( PolyValueDeserializer::deserializeValueMap )
                .collect( Collectors.toList() );
    }

}
