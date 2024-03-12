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

package org.polypheny.db.cypher.pattern;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.util.Pair;

@Getter
public class CypherNodePattern extends CypherPattern {

    private final CypherVariable variable;
    @Getter
    private final List<PolyString> labels;
    private final List<ParserPos> positions;

    @Nullable
    private final CypherExpression properties;
    private final CypherExpression predicate;


    public CypherNodePattern( ParserPos pos, CypherVariable variable, List<StringPos> labels, CypherExpression properties, CypherExpression predicate ) {
        super( pos );
        this.variable = variable;
        this.labels = PolyList.copyOf( labels.stream().map( StringPos::getImage ).map( PolyString::of ).collect( Collectors.toList() ) );
        this.positions = labels.stream().map( StringPos::getPos ).toList();
        this.properties = properties;
        this.predicate = predicate;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.NODE_PATTERN;
    }


    public Pair<PolyString, PolyNode> getPolyNode() {
        PolyDictionary properties = this.properties != null ? (PolyDictionary) this.properties.getComparable() : new PolyDictionary();

        String name = null;
        if ( variable != null ) {
            name = variable.getName();
        }

        return Pair.of( PolyString.of( name ), new PolyNode( properties, PolyList.copyOf( labels ), PolyString.of( name ) ) );
    }

}
