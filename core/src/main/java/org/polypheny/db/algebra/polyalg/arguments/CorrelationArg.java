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

package org.polypheny.db.algebra.polyalg.arguments;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;

public class CorrelationArg implements PolyAlgArg {

    @Getter
    private final CorrelationId corrId;


    public CorrelationArg( CorrelationId corrId ) {
        this.corrId = corrId;
    }


    @Override
    public ParamType getType() {
        return ParamType.CORR_ID;
    }


    @Override
    public String toPolyAlg( AlgNode context, @NonNull List<String> inputFieldNames ) {
        return String.valueOf( corrId.getId() );
    }


    @Override
    public ObjectNode serialize( AlgNode context, @NonNull List<String> inputFieldNames, ObjectMapper mapper ) {
        return mapper.createObjectNode().put( "arg", corrId.getId() );
    }

}
