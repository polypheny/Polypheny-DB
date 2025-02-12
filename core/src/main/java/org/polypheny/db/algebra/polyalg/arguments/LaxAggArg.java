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
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;

public class LaxAggArg implements PolyAlgArg {

    @Getter
    private final LaxAggregateCall agg;


    public LaxAggArg( LaxAggregateCall agg ) {
        this.agg = agg;
    }


    @Override
    public ParamType getType() {
        return ParamType.LAX_AGGREGATE;
    }


    @Override
    public String toPolyAlg( AlgNode context, @NonNull List<String> inputFieldNames ) {
        String str = aggToString( inputFieldNames );
        return PolyAlgUtils.appendAlias( str, agg.name );
    }


    private String aggToString( List<String> inputFieldNames ) {
        StringBuilder sb = new StringBuilder( agg.function.toString() );
        sb.append( "(" );
        if ( agg.getInput().isPresent() ) {
            sb.append( PolyAlgUtils.digestWithNames( agg.getInput().get(), inputFieldNames ) );
        }
        sb.append( ")" );
        return sb.toString();
    }


    @Override
    public ObjectNode serialize( AlgNode context, @NonNull List<String> inputFieldNames, ObjectMapper mapper ) {
        ObjectNode node = mapper.createObjectNode();
        node.put( "function", agg.function.toString() );
        if ( agg.getInput().isPresent() ) {
            node.put( "input", PolyAlgUtils.digestWithNames( agg.getInput().get(), inputFieldNames ) );
        }
        node.put( "alias", agg.name );
        return node;
    }

}
