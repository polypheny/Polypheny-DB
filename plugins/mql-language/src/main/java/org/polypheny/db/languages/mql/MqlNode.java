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

package org.polypheny.db.languages.mql;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.util.Litmus;


@Getter
public abstract class MqlNode implements Node {

    protected final ParserPos pos;

    @Nullable
    public final String namespace;

    @Setter
    List<String> stores = new ArrayList<>();

    @Setter
    List<String> primary = new ArrayList<>();


    @Override
    public QueryLanguage getLanguage() {
        return QueryLanguage.from( "mongo" );
    }


    protected MqlNode( ParserPos pos, @Nullable String namespace ) {
        this.pos = pos;
        this.namespace = namespace;
    }


    protected BsonDocument getDocumentOrNull( BsonDocument document, String name ) {
        if ( document != null && document.containsKey( name ) && document.get( name ).isDocument() ) {
            return document.getDocument( name );
        } else {
            return null;
        }
    }


    protected BsonArray getArrayOrNull( BsonDocument document, String name ) {
        if ( document != null && document.containsKey( name ) && document.get( name ).isArray() ) {
            return document.getBoolean( name ).asArray();
        } else {
            return null;
        }
    }


    protected boolean getBoolean( BsonDocument document, String name ) {
        if ( document != null && document.containsKey( name ) && document.get( name ).isBoolean() ) {
            return document.getBoolean( name ).asBoolean().getValue();
        } else {
            return false;
        }
    }


    @Override
    public @Nullable String getNamespaceName() {
        return namespace;
    }


    @Override
    public @Nullable String getEntity() {
        return null;
    }


    @Override
    public Object clone() {
        return null;
    }


    @Override
    public Node clone( ParserPos pos ) {
        return null;
    }


    public abstract Mql.Type getMqlKind();


    @Override
    public boolean isA( Set<Kind> category ) {
        return category.contains( this.getKind() );
    }


    public Mql.Family getFamily() {
        return Mql.getFamily( getMqlKind() );
    }


    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{}";
    }


    @Override
    public boolean equalsDeep( Node node, Litmus litmus ) {
        return false;
    }


    @Override
    public Kind getKind() {
        return switch ( getFamily() ) {
            case DCL -> Kind.OTHER;
            case DDL -> Kind.OTHER_DDL;
            case DML -> Kind.INSERT;
            case DQL -> Kind.SELECT;
            default -> Kind.OTHER;
        };
    }


    @Override
    public <R> R accept( NodeVisitor<R> visitor ) {
        return null;
    }

}
