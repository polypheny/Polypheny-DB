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

package org.polypheny.db.catalog;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.util.Litmus;

public class MockIdentifier implements Identifier, Node {

    private final String name;
    @Getter
    private final ParserPos pos;


    public MockIdentifier( String name, ParserPos pos ) {
        this.name = name;
        this.pos = pos;
    }


    @Override
    public String getSimple() {
        return name;
    }


    @Override
    public boolean isStar() {
        return false;
    }


    @Override
    public boolean isSimple() {
        return false;
    }


    @Override
    public List<String> getNames() {
        return Collections.singletonList( name );
    }


    @Override
    public Node clone( ParserPos pos ) {
        return null;
    }


    @Override
    public Kind getKind() {
        return Kind.IDENTIFIER;
    }


    @Override
    public QueryLanguage getLanguage() {
        return QueryLanguage.from( "sql" );
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
    public @Nullable String getEntity() {
        return null;
    }


    @Override
    public <R> R accept( NodeVisitor<R> visitor ) {
        return null;
    }

}
