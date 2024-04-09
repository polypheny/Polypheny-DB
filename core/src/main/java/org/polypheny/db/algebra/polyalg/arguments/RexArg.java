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

import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;
import org.polypheny.db.rex.RexNode;

public class RexArg implements PolyAlgArg {

    public static final RexArg NULL = new RexArg( null );

    @Getter
    private final RexNode node;
    private final String alias;
    private boolean omitTrue = false;


    /**
     * Use this constructor if you want to specify an alias on level of a single argument.
     * If this argument is part of a ListArg, it is often more convenient to specify all aliases
     * as a single list in the ListArg constructor. In that case, alias should be {@code null}.
     *
     * @param node the RexNode corresponding to this argument
     * @param alias the alias name of this argument or {@code null} if no alias should be used.
     */
    public RexArg( RexNode node, String alias ) {
        this.node = node;
        this.alias = alias;
    }


    public RexArg( RexNode node ) {
        this( node, null );
    }


    public RexArg( RexNode node, boolean omitTrue ) {
        this( node );
        this.omitTrue = omitTrue;
    }


    @Override
    public ParamType getType() {
        return ParamType.SIMPLE_REX;
    }


    @Override
    public String toPolyAlg( AlgNode context, @NonNull List<String> inputFieldNames ) {
        return PolyAlgUtils.appendAlias( rexAsString( inputFieldNames ), alias );
    }


    private String rexAsString( @NonNull List<String> inputFieldNames ) {
        String str = node == null ? "" : node.toString();
        if ( omitTrue && str.equals( "true" ) ) {
            return "";
        }
        if ( inputFieldNames.isEmpty() || node == null ) {
            return str;
        }
        return PolyAlgUtils.digestWithNames( node, inputFieldNames );
    }

}
