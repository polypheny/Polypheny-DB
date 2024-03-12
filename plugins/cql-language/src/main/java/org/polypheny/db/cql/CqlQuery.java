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

package org.polypheny.db.cql;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.cql.BooleanGroup.FieldOpsBooleanOperator;
import org.polypheny.db.cql.utils.Tree;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Pair;


/**
 * Packaging all the information in a CQL query together.
 */
public record CqlQuery(
        Tree<Combiner, EntityIndex> queryRelation,
        Tree<BooleanGroup<FieldOpsBooleanOperator>, Filter> filters,
        Map<String, EntityIndex> tableIndexMapping,
        Map<String, FieldIndex> columnIndexMapping,
        List<Pair<FieldIndex, Map<String, Modifier>>> sortSpecifications,
        Projections projections
) implements Node {


    @Override
    public Node clone( ParserPos pos ) {
        return null;
    }


    @Override
    public Kind getKind() {
        return Kind.SELECT;
    }


    @Override
    public QueryLanguage getLanguage() {
        return QueryLanguage.from( "cql" );
    }


    @Override
    public boolean isA( Set<Kind> category ) {
        return false;
    }


    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        if ( filters != null ) {
            stringBuilder.append( filters );
        }
        stringBuilder.append( "relation " ).append( queryRelation );
        if ( sortSpecifications != null && !sortSpecifications.isEmpty() ) {
            stringBuilder.append( " " );
            for ( Pair<FieldIndex, Map<String, Modifier>> sortSpecification : sortSpecifications ) {
                stringBuilder.append( sortSpecification.toString() );
            }
            stringBuilder.append( " " );
        }

        return stringBuilder.toString();
    }


    @Override
    public ParserPos getPos() {
        return null;
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
