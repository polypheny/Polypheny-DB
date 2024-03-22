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

package org.polypheny.db.algebra.polyalg.arguments;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;

public class AggArg implements PolyAlgArg {

    private final AggregateCall agg;


    public AggArg( AggregateCall agg ) {
        this.agg = agg;
    }


    @Override
    public ParamType getType() {
        return ParamType.AGGREGATE;
    }


    @Override
    public String toPolyAlg() {
        return toPolyAlg( null );
    }


    @Override
    public String toPolyAlg( AlgNode context ) {
        String str = agg.toString( PolyAlgUtils.getFieldNames( context ) );
        String name = agg.getName();
        if ( name == null ) {
            // TODO: make sure agg.getName() is never null
            Aggregate instance = (Aggregate) context;
            int i = instance.getAggCallList().indexOf( agg );
            if ( i != -1 ) {
                i += instance.getGroupSet().asList().size();
            }
            name = "$f" + i;
        }
        return PolyAlgUtils.appendAlias( str, name );
    }

}
