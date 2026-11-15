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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Window.Group;
import org.polypheny.db.algebra.core.Window.RexWinAggCall;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;

public class WindowGroupArg implements PolyAlgArg {

    @Getter
    private final Group group;


    public WindowGroupArg( Group group ) {
        this.group = group;
    }


    @Override
    public ParamType getType() {
        return ParamType.WINDOW_GROUP;
    }


    @Override
    public String toPolyAlg( AlgNode context, @NonNull List<String> inputFieldNames ) {
        throw new NotImplementedException( "WindowGroupArg can not yet be serialized to PolyAlgebra" );
    }


    @Override
    public ObjectNode serialize( AlgNode context, @NonNull List<String> inputFieldNames, ObjectMapper mapper ) {
        ObjectNode node = mapper.createObjectNode();

        node.put( "isRows", group.isRows );
        node.put( "lowerBound", group.lowerBound.toString() );
        node.put( "upperBound", group.upperBound.toString() );

        ArrayNode aggCalls = mapper.createArrayNode();
        for ( RexWinAggCall call : group.aggCalls ) {
            aggCalls.add( PolyAlgUtils.digestWithNames( call, inputFieldNames ) );
        }
        node.set( "aggCalls", aggCalls );

        ArrayNode collList = mapper.createArrayNode();
        for ( AlgFieldCollation coll : group.orderKeys.getFieldCollations() ) {
            collList.add( CollationArg.serialize( coll, inputFieldNames, mapper ) );
        }
        node.set( "orderKeys", collList );
        return node;
    }

}
