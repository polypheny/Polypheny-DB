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

/**
 * One-size-fits-all class for any RexNodes or even literals not wrapped in a RexNode
 */

@Getter
public class PolyAlgExpression extends PolyAlgNode {

    private final List<PolyAlgLiteral> literals;
    private final List<PolyAlgExpression> childExps;
    private final String cast;


    public PolyAlgExpression( @NonNull List<PolyAlgLiteral> literals, List<PolyAlgExpression> childExps, String cast, ParserPos pos ) {
        super( pos );
        assert !literals.isEmpty();

        this.literals = literals;
        this.childExps = childExps;
        this.cast = cast;

    }


    public boolean isCall() {
        // if childExps is an empty list, we have a call with 0 arguments
        return childExps == null;
    }

    public boolean hasCast() {
        return cast != null;
    }

}
