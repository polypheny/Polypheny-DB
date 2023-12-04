/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.algebra.core;

import java.util.Optional;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.rex.RexNode;

public class DocumentAggregateCall {

    public final String name;
    public final AggFunction function;
    private final RexNode input;


    public DocumentAggregateCall( String name, AggFunction function, RexNode input ) {
        this.name = name;
        this.function = function;
        this.input = input;
    }


    public static DocumentAggregateCall create( String name, AggFunction function, RexNode input ) {
        return new DocumentAggregateCall( name, function, input );
    }


    public Optional<RexNode> getInput() {
        return Optional.ofNullable( input );
    }

}
