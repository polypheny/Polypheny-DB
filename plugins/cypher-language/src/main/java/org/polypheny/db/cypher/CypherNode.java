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

package org.polypheny.db.cypher;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.cypher.clause.CypherCreate;
import org.polypheny.db.cypher.query.CypherPeriodicCommit;
import org.polypheny.db.cypher.query.CypherSingleQuery;
import org.polypheny.db.cypher.query.CypherUnion;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.Litmus;

@Getter
public abstract class CypherNode implements Node {

    public final ParserPos pos;

    public static final List<CypherKind> DDL = ImmutableList.of( CypherKind.CREATE_DATABASE, CypherKind.DROP, CypherKind.ADMIN_COMMAND );


    protected CypherNode( ParserPos pos ) {
        this.pos = pos;
    }


    @Override
    public Kind getKind() {
        return isDdl() ? Kind.OTHER_DDL : Kind.OTHER;
    }


    public abstract CypherKind getCypherKind();


    public boolean isFullScan() {
        return false;
    }


    @Override
    public Node clone( ParserPos pos ) {
        return null;
    }


    @Override
    public QueryLanguage getLanguage() {
        return QueryLanguage.from( "cypher" );
    }


    @Override
    public boolean isA( Set<Kind> category ) {
        return false;
    }


    @Override
    public boolean equalsDeep( Node node, Litmus litmus ) {
        return false;
    }


    @Override
    public <R> R accept( NodeVisitor<R> visitor ) {
        return null;
    }


    public void accept( CypherVisitor visitor ) {
        throw new UnsupportedOperationException( "Accept() method was not implemented for the used cypher node: " + getClass().getSimpleName() );
    }


    @Override
    public boolean isDdl() {
        return DDL.contains( getCypherKind() );
    }


    @Nullable
    public static String getNameOrNull( CypherSimpleEither<String, CypherParameter> input ) {
        if ( input == null ) {
            return null;
        }
        if ( input.getLeft() != null ) {
            return input.getLeft();
        }
        return input.getRight().getName();

    }


    @Override
    public @Nullable String getEntity() {
        return null;
    }


    public List<PolyString> getUnderlyingLabels() {
        return List.of();
    }


    public enum CypherKind {
        SCOPE,
        REMOVE,
        ADMIN_COMMAND,
        QUERY,
        PATTERN,
        EXPRESSION,
        WITH_GRAPH,
        CALL,
        CASE,
        CREATE,
        CREATE_DATABASE,
        SCHEMA_COMMAND,
        ADMIN_ACTION,
        DELETE,
        DROP,
        FOR_EACH,
        LOAD_CSV,
        MATCH,
        MERGE,
        ORDER_ITEM,
        RETURN,
        SET,
        SHOW,
        TRANSACTION,
        UNWIND,
        USE,
        WAIT,
        WHERE,
        WITH,
        MAP_PROJECTION,
        YIELD,
        EITHER,
        RESOURCE,
        PRIVILEGE,
        PATH_LENGTH,
        CALL_RESULT,
        HINT,
        PATH,
        PERIODIC_COMMIT,
        UNION,
        SINGLE,
        NAMED_PATTERN,
        NODE_PATTERN,
        REL_PATTERN,
        SHORTEST_PATTERN,
        LITERAL,
        FULL,
        SET_ITEM
    }


    public enum CypherFamily {
        QUERY( CypherKind.QUERY, CypherKind.PERIODIC_COMMIT, CypherKind.UNION, CypherKind.SINGLE ),
        PATTERN( CypherKind.PATH, CypherKind.PATTERN, CypherKind.NODE_PATTERN, CypherKind.REL_PATTERN, CypherKind.SHORTEST_PATTERN );

        private final List<CypherKind> kinds;


        CypherFamily( CypherKind... kinds ) {
            this.kinds = List.of( kinds );
        }


        public boolean contains( CypherKind kind ) {
            return kinds.contains( kind );
        }
    }


    public static abstract class CypherVisitor {

        public void visit( CypherSingleQuery query ) {
            query.getClauses().forEach( c -> c.accept( this ) );
        }


        public void visit( CypherPeriodicCommit query ) {
            query.getQueryBody().forEach( c -> c.accept( this ) );
        }


        public void visit( CypherUnion union ) {
            union.getLeft().accept( this );
            union.getRight().accept( this );
        }


        public void visit( CypherCreate create ) {
            create.getPatterns().forEach( p -> p.accept( this ) );
        }

    }

}
