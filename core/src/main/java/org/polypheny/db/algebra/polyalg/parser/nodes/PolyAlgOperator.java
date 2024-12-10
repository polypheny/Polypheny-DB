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

package org.polypheny.db.algebra.polyalg.parser.nodes;

import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.languages.ParserPos;

@Getter
public class PolyAlgOperator extends PolyAlgNode {

    private final String opName;
    private final List<PolyAlgNamedArgument> arguments;
    private final List<PolyAlgOperator> children;


    public PolyAlgOperator( String opName, List<PolyAlgNamedArgument> arguments, List<PolyAlgOperator> children, ParserPos pos ) {
        super( pos );

        this.opName = opName;
        this.arguments = arguments == null ? List.of() : arguments;
        this.children = children == null ? List.of() : children;
    }

}
