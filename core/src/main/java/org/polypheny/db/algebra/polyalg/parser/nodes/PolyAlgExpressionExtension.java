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
import java.util.StringJoiner;
import lombok.Getter;

@Getter
public class PolyAlgExpressionExtension {

    private final List<PolyAlgLiteral> literals;
    private final ExtensionType type;


    public PolyAlgExpressionExtension( List<PolyAlgLiteral> literals, ExtensionType type ) {
        this.literals = literals;
        this.type = type;
    }


    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner( " " );
        for ( PolyAlgLiteral literal : literals ) {
            joiner.add( literal.toString() );
        }
        return joiner.toString();
    }


    public enum ExtensionType {
        FILTER,
        WITHIN_GROUP,
        APPROXIMATE,
        WITH,
        OVER;
    }

}