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

package org.polypheny.db.cypher.clause;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.cypher.pattern.CypherEveryPathPattern;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.Statement;

@Getter
public class CypherCreate extends CypherClause implements ExecutableStatement {

    private final List<CypherPattern> patterns;


    public CypherCreate( ParserPos pos, List<CypherPattern> patterns ) {
        super( pos );
        this.patterns = patterns;
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.CREATE;
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        List<CypherEveryPathPattern> paths = this.patterns
                .stream()
                .filter( p -> p.getCypherKind() == CypherKind.PATH )
                .map( p -> (CypherEveryPathPattern) p )
                .collect( Collectors.toList() );

        Set<String> relLabels = paths
                .stream()
                .map( CypherEveryPathPattern::getRelationships )
                .flatMap( rs -> rs.stream().flatMap( r -> r.getLabels().stream() ) )
                .collect( Collectors.toSet() );

        Set<String> nodeLabels = paths
                .stream()
                .map( CypherEveryPathPattern::getNodes )
                .flatMap( rs -> rs.stream().flatMap( n -> n.getLabels().stream() ) )
                .collect( Collectors.toSet() );

    }

}
