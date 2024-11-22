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

package org.polypheny.db.cypher.clause;

import java.util.List;
import java.util.stream.Stream;
import lombok.Getter;
import org.polypheny.db.cypher.expression.CypherLiteral;
import org.polypheny.db.cypher.expression.CypherLiteral.Literal;
import org.polypheny.db.cypher.pattern.CypherEveryPathPattern;
import org.polypheny.db.cypher.pattern.CypherNodePattern;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.cypher.pattern.CypherRelPattern;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.transaction.locking.EntityIdentifierGenerator;
import org.polypheny.db.transaction.locking.EntityIdentifierUtils;


@Getter
public class CypherCreate extends CypherClause {

    private final List<CypherPattern> patterns;


    public CypherCreate( ParserPos pos, List<CypherPattern> patterns ) {
        super( pos );
        this.patterns = patterns;
        addEntryIdentifiers();
    }


    private void addEntryIdentifiers() {
        patterns.stream()
                .filter( CypherEveryPathPattern.class::isInstance )
                .map( CypherEveryPathPattern.class::cast )
                .forEach( everyPathPattern -> Stream.concat(
                        everyPathPattern.getNodes().stream(),
                        everyPathPattern.getEdges().stream()
                ).forEach( pattern -> {
                    CypherLiteral properties = extractProperties( pattern );
                    properties.getMapValue().put(
                            EntityIdentifierUtils.IDENTIFIER_KEY,
                            new CypherLiteral(
                                    ParserPos.ZERO,
                                    Literal.DECIMAL,
                                    String.valueOf( EntityIdentifierGenerator.INSTANCE.getEntryIdentifier() ),
                                    false
                            )
                    );
                } ) );
    }

    private CypherLiteral extractProperties( CypherPattern pattern ) {
        if ( pattern instanceof CypherNodePattern cypherNodePattern ) {
            if (cypherNodePattern.getProperties() == null) {
                cypherNodePattern.initializeProperties();
            }
            return (CypherLiteral) cypherNodePattern.getProperties();
        }
        if ( pattern instanceof CypherRelPattern cypherRelPattern ) {
            if (cypherRelPattern.getProperties() == null) {
                cypherRelPattern.initializeProperties();
            }
            return (CypherLiteral) cypherRelPattern.getProperties();
        }
        throw new RuntimeException( "Unknown pattern type" );
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.CREATE;
    }


    @Override
    public void accept( CypherVisitor visitor ) {
        visitor.visit( this );
    }

}
