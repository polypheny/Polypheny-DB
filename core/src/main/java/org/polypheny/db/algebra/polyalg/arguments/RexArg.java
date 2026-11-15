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
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;
import org.polypheny.db.rex.RexNode;

public class RexArg implements PolyAlgArg {

    public static final RexArg NULL = new RexArg( null );

    @Getter
    private final RexNode node;
    @Getter
    private final String alias;
    private List<String> inputFieldNames = null;


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
        this( node, (String) null );
    }


    /**
     * Creates a RexArg for the given RexNode that uses the specified list of inputFieldNames during serialization.
     * This can be useful for leaf nodes, since the inputFieldNames cannot be derived from the child node.
     *
     * @param node the RexNode corresponding to this argument
     * @param inputFieldNames the list of names to be used for serialization
     */
    public RexArg( RexNode node, @NonNull List<String> inputFieldNames ) {
        this( node );
        this.inputFieldNames = inputFieldNames;
    }


    @Override
    public ParamType getType() {
        return ParamType.REX;
    }


    @Override
    public String toPolyAlg( AlgNode context, @NonNull List<String> inputFieldNames ) {
        return PolyAlgUtils.appendAlias( rexAsString( inputFieldNames ), alias );
    }


    private String rexAsString( @NonNull List<String> inputFieldNames ) {
        String str = node == null ? "" : node.toString();
        if ( node == null ) {
            return str;
        }
        return PolyAlgUtils.digestWithNames( node, this.inputFieldNames == null ? inputFieldNames : this.inputFieldNames );
    }


    @Override
    public ObjectNode serialize( AlgNode context, @NonNull List<String> inputFieldNames, ObjectMapper mapper ) {
        ObjectNode node = mapper.createObjectNode();
        node.put( "rex", rexAsString( inputFieldNames ) );
        if ( alias != null ) {
            node.put( "alias", alias );
        }
        return node;
    }

}
