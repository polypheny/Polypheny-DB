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
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;
import org.polypheny.db.rex.RexNode;

public class RexArg implements PolyAlgArg {

    private final RexNode node;
    private boolean omitTrue = false;


    public RexArg( RexNode node ) {
        this.node = node;
    }

    public RexArg( RexNode node, boolean omitTrue ) {
        this(node);
        this.omitTrue = omitTrue;
    }


    @Override
    public ParamType getType() {
        return ParamType.SIMPLE_REX;
    }


    @Override
    public String toPolyAlg() {
        return toPolyAlg( null );
    }


    @Override
    public String toPolyAlg( AlgNode context ) {
        String str = node == null ? "" : node.toString();
        if (omitTrue && str.equals( "true" )) {
            return "";
        }
        if ( context == null || node == null ) {
            return str;
        }
        return PolyAlgUtils.digestWithNames( node, context );
    }

}