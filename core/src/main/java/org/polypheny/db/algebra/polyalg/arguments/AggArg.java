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
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;

public class AggArg implements PolyAlgArg {

    @Getter
    private final AggregateCall agg;


    public AggArg( AggregateCall agg ) {
        this.agg = agg;
    }


    @Override
    public ParamType getType() {
        return ParamType.AGGREGATE;
    }


    @Override
    public String toPolyAlg( AlgNode context, @NonNull List<String> inputFieldNames ) {
        String str = aggToString( inputFieldNames );
        return PolyAlgUtils.appendAlias( str, getAggName( context ) );
    }


    private String aggToString( List<String> inputFieldNames ) {
        StringBuilder buf = new StringBuilder( agg.getAggregation().getOperatorName().toString() );
        buf.append( "(" );
        List<Integer> argList = agg.getArgList();
        AlgCollation collation = agg.getCollation();

        if ( agg.isDistinct() ) {
            buf.append( argList.isEmpty() ? "DISTINCT" : "DISTINCT " );
        }
        int i = -1;
        for ( Integer arg : argList ) {
            if ( ++i > 0 ) {
                buf.append( ", " );
            }
            buf.append( inputFieldNames.get( arg ) );
        }
        buf.append( ")" );
        if ( agg.isApproximate() ) {
            buf.append( " APPROXIMATE" );
        }
        if ( !collation.equals( AlgCollations.EMPTY ) ) {
            throw new NotImplementedException( "Aggs using the WITHIN GROUP statement are not yet supported." );
            /*
            buf.append( " WITHIN GROUP (" );
            buf.append( collation );
            buf.append( ")" );
            */
        }
        if ( agg.hasFilter() ) {
            buf.append( " FILTER " );
            buf.append( inputFieldNames.get( agg.filterArg ) );
        }
        return buf.toString();
    }


    private String getAggName( AlgNode context ) {
        String name = agg.getName();
        if ( name == null ) {
            Aggregate instance = (Aggregate) context;
            int i = instance.getAggCallList().indexOf( agg );
            if ( i != -1 ) {
                i += instance.getGroupSet().asList().size();
            }
            name = "$f" + i;
        }
        return name;
    }


    @Override
    public ObjectNode serialize( AlgNode context, @NonNull List<String> inputFieldNames, ObjectMapper mapper ) {
        ObjectNode node = mapper.createObjectNode();
        node.put( "function", agg.getAggregation().getOperatorName().toString() );
        node.put( "distinct", agg.isDistinct() );
        node.put( "approximate", agg.isApproximate() );

        ArrayNode argList = mapper.createArrayNode();
        for ( int idx : agg.getArgList() ) {
            argList.add( inputFieldNames.get( idx ) );
        }
        node.set( "argList", argList );

        ArrayNode collList = mapper.createArrayNode();
        for ( AlgFieldCollation coll : agg.getCollation().getFieldCollations() ) {
            collList.add( CollationArg.serialize( coll, inputFieldNames, mapper ) );
        }
        node.set( "collList", collList );
        if ( agg.hasFilter() ) {
            node.put( "filter", inputFieldNames.get( agg.filterArg ) );
        }

        node.put( "alias", getAggName( context ) );
        return node;
    }

}
