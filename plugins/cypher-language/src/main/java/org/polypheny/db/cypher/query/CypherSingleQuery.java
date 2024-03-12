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

package org.polypheny.db.cypher.query;

import java.util.List;
import lombok.Getter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.CypherNode;
import org.polypheny.db.cypher.clause.CypherClause;
import org.polypheny.db.cypher.clause.CypherQuery;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyString;

@Getter
public class CypherSingleQuery extends CypherQuery implements ExecutableStatement {

    private final List<CypherClause> clauses;


    public CypherSingleQuery( ParserPos pos, List<CypherClause> clauses ) {
        super( pos );
        this.clauses = clauses;
    }


    @Override
    public boolean isFullScan() {
        return clauses.stream().anyMatch( CypherNode::isFullScan );
    }


    public CypherSingleQuery( List<CypherClause> clauses ) {
        this( ParserPos.ZERO, clauses );
    }


    @Override
    public void accept( CypherVisitor visitor ) {
        visitor.visit( this );
    }


    @Override
    public CypherKind getCypherKind() {
        return CypherKind.SINGLE;
    }


    @Override
    public boolean isDdl() {
        if ( clauses.stream().allMatch( CypherNode::isDdl ) ) {
            return true;
        }
        if ( clauses.stream().noneMatch( CypherNode::isDdl ) ) {
            return false;
        }
        throw new GenericRuntimeException( "The mixed query is not supported" );
    }


    @Override
    public List<PolyString> getUnderlyingLabels() {
        return clauses.stream().map( CypherNode::getUnderlyingLabels ).flatMap( List::stream ).collect( java.util.stream.Collectors.toList() );
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        for ( CypherClause clause : clauses ) {
            if ( clause.isDdl() ) {
                ((ExecutableStatement) clause).execute( context, statement, parsedQueryContext );
            }
        }
    }

}
