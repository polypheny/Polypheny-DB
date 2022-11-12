/*
 * Copyright 2019-2022 The Polypheny Project
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

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.polypheny.db.cypher.CypherPathLength;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.runtime.PolyCollections.PolyDictionary;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.util.Pair;

@Getter
public class CypherRelPattern extends CypherPattern {

    private final boolean left;
    private final boolean right;
    private final CypherVariable variable;
    private final List<StringPos> relTypes;
    private final CypherPathLength pathLength;
    @Nullable
    private final CypherExpression properties;
    private final CypherExpression predicate;
    private final boolean legacyTypeSeparator;


    public CypherRelPattern( ParserPos pos, boolean left, boolean right, CypherVariable variable, List<StringPos> relTypes, CypherPathLength pathLength, CypherExpression properties, CypherExpression predicate, boolean legacyTypeSeparator ) {
        super( pos );
        this.left = left;
        this.right = right;
        this.variable = variable;
        this.relTypes = relTypes;
        this.pathLength = pathLength;
        this.properties = properties;
        this.predicate = predicate;
        this.legacyTypeSeparator = legacyTypeSeparator;
    }


    public List<String> getLabels() {
        return relTypes.stream().map( StringPos::getImage ).collect( Collectors.toList() );
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.REL_PATTERN;
    }


    public Pair<String, PolyEdge> getPolyEdge( String leftId, String rightId ) {
        PolyDictionary properties = this.properties != null ? (PolyDictionary) this.properties.getComparable() : new PolyDictionary();
        EdgeDirection direction = left == right ? EdgeDirection.NONE : right ? EdgeDirection.LEFT_TO_RIGHT : EdgeDirection.RIGHT_TO_LEFT;
        List<String> labels = relTypes.stream().map( StringPos::getImage ).collect( Collectors.toList() );

        String name = null;
        if ( variable != null ) {
            name = variable.getName();
        }

        PolyEdge edge = new PolyEdge( properties, ImmutableList.copyOf( labels ), leftId, rightId, direction, name );
        if ( pathLength != null ) {
            edge.fromTo( Pair.of( Integer.parseInt( pathLength.getFrom() ), Integer.parseInt( pathLength.getTo() ) ) );
        }

        return Pair.of( name, edge );
    }

}
