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
import lombok.NonNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;

public interface PolyAlgArg {

    /**
     * Return the ParamType this argument is meant for.
     * In case of multivalued arguments like lists, this returns the type of the underlying elements.
     * If this type can not be inferred (e.g. because it is an empty list), the inherent type of the multivalued argument
     * (i.e. ParamType.LIST) is returned.
     *
     * @return the ParamType of this argument or its children in case of multivalued arguments.
     */

    ParamType getType();

    /**
     * Returns the PolyAlg representation of this argument.
     *
     * @param context the AlgNode this argument belongs to
     * @param inputFieldNames list of field names of all children with duplicate names uniquified
     * @return string representing the PolyAlg representation of this argument under the given context
     */
    String toPolyAlg( AlgNode context, @NonNull List<String> inputFieldNames );


    /**
     * Returns the JSON-serialized PolyAlg representation of this argument.
     * While implementations are free to decide the structure of the ObjectNode,
     * one convention is that if the argument has an alias name, then it should be set
     * with the key {@code alias}. This ensures that the alias can also be set by a ListArg wrapping this argument.
     *
     * @param context the AlgNode this argument belongs to
     * @param inputFieldNames list of field names of all children with duplicate names uniquified
     * @param mapper the ObjectMapper used for creating JsonNodes
     * @return a ObjectNode representing the JSON-serialized PolyAlg representation of this argument under the given context
     */
    ObjectNode serialize( AlgNode context, @NonNull List<String> inputFieldNames, ObjectMapper mapper );

    /**
     * Returns a wrapped PolyAlg representation of this argument.
     * This is useful as it introduces a common structure for all implementations.
     * The wrapped representation adds attributes for inferring the structure of the wrapped argument (like its type).
     * The serialized argument itself (whose structure can vary) can be found under the key {@code value}.
     *
     * @param context the AlgNode this argument belongs to
     * @param inputFieldNames list of field names of all children with duplicate names uniquified
     * @param mapper the ObjectMapper used for creating JsonNodes
     * @return a ObjectNode representing the JSON-serialized PolyAlg representation of this argument under the given context
     */
    default ObjectNode serializeWrapped( AlgNode context, @NonNull List<String> inputFieldNames, ObjectMapper mapper ) {
        String type = this.getType().name();
        ObjectNode argNode = mapper.createObjectNode();
        argNode.put( "type", type );
        argNode.set( "value", this.serialize( context, inputFieldNames, mapper ) );
        return argNode;
    }

}
