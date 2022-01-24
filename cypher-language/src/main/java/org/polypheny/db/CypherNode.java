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

package org.polypheny.db;

import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.util.Litmus;

public abstract class CypherNode implements Node {

    protected final static CypherNode EMPTY_CYPHER = new EmptyNode();

    protected final static CypherNode STAR = new StarNode();

    @Getter
    protected final ParserPos pos;

    @Getter
    @Setter
    private CypherNode input;


    protected CypherNode( ParserPos pos, CypherNode input ) {
        this.pos = pos;
        this.input = input;
    }


    @Override
    public Kind getKind() {
        return Kind.OTHER;
    }


    @Override
    public Node clone( ParserPos pos ) {
        return null;
    }


    @Override
    public QueryLanguage getLanguage() {
        return QueryLanguage.Cypher;
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


    public boolean isStar() {
        return false;
    }


    public boolean isEmpty() {
        return false;
    }


    private static class EmptyNode extends CypherNode {


        protected EmptyNode() {
            super( ParserPos.ZERO, null );
        }


        @Override
        public boolean isEmpty() {
            return true;
        }

    }


    private static class StarNode extends CypherNode {

        protected StarNode() {
            super( ParserPos.ZERO, null );
        }


        @Override
        public boolean isStar() {
            return true;
        }

    }

}
