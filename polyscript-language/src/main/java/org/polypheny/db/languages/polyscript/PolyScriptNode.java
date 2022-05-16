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

package org.polypheny.db.languages.polyscript;

import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.util.Litmus;

import java.util.Set;

public class PolyScriptNode implements Node {
    private final String statement;
    private PolyScriptNode next;

    public PolyScriptNode(String statement) {
        this.statement = statement;
    }

    @Override
    public Node clone(ParserPos pos) {
        return null;
    }

    @Override
    public Kind getKind() {
        return Kind.PROCEDURE_CALL;
    }

    @Override
    public Catalog.QueryLanguage getLanguage() {
        return Catalog.QueryLanguage.POLYSCRIPT;
    }

    @Override
    public boolean isA(Set<Kind> category) {
        return getKind().belongsTo( category );
    }

    @Override
    public ParserPos getPos() {
        return null;
    }

    @Override
    public boolean equalsDeep(Node node, Litmus litmus) {
        return false;
    }

    @Override
    public <R> R accept(NodeVisitor<R> visitor) {
        return null;
    }
}
