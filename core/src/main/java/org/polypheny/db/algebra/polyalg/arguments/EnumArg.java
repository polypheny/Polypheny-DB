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

public class EnumArg<E extends Enum<E>> implements PolyAlgArg {

    @Getter
    private final E arg;
    private final ParamType type;


    public EnumArg( E arg, ParamType enumType ) {
        assert enumType.isEnum();

        this.arg = arg;
        this.type = enumType;
    }


    @Override
    public ParamType getType() {
        return type;
    }


    @Override
    public String toPolyAlg( AlgNode context, @NonNull List<String> inputFieldNames ) {
        return arg.name();
    }


    @Override
    public ObjectNode serialize( AlgNode context, @NonNull List<String> inputFieldNames, ObjectMapper mapper ) {
        ObjectNode node = mapper.createObjectNode();
        node.put( "arg", arg.name() );
        node.put( "enum", arg.getDeclaringClass().getSimpleName() );
        return node;
    }


    @Override
    public ObjectNode serializeWrapped( AlgNode context, @NonNull List<String> inputFieldNames, ObjectMapper mapper ) {
        ObjectNode node = PolyAlgArg.super.serializeWrapped( context, inputFieldNames, mapper );
        return node.put( "isEnum", true );
    }

}
