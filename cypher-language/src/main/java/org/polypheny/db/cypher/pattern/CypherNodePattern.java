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

import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.runtime.PolyCollections.PolyDirectory;
import org.polypheny.db.schema.graph.PolyNode;

@Getter
public class CypherNodePattern extends CypherPattern {

    private final CypherVariable variable;
    private final List<StringPos> labels;
    private final CypherExpression properties;
    private final CypherExpression predicate;


    public CypherNodePattern( ParserPos pos, CypherVariable variable, List<StringPos> labels, CypherExpression properties, CypherExpression predicate ) {
        super( pos );
        this.variable = variable;
        this.labels = labels;
        this.properties = properties;
        this.predicate = predicate;
    }


    public List<String> getLabels() {
        return labels.stream().map( StringPos::getImage ).collect( Collectors.toList() );
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.NODE_PATTERN;
    }


    public PolyNode getPolyNode() {
        PolyDirectory properties = (PolyDirectory) this.properties.getComparable();

        return new PolyNode( properties );
    }

}
