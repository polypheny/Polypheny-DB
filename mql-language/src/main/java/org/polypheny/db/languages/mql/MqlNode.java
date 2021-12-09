/*
 * Copyright 2019-2021 The Polypheny Project
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
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.util.Litmus;


public abstract class MqlNode implements Node {

    @Getter
    protected final ParserPos pos;

    @Getter
    @Setter
    List<String> stores = new ArrayList<>();

    @Setter
    @Getter
    List<String> primary = new ArrayList<>();


    @Override
    public QueryLanguage getLanguage() {
        return QueryLanguage.MONGO_QL;
    }


    protected MqlNode( ParserPos pos ) {
        this.pos = pos;
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
        return false;
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
        return Kind.OTHER;
    }


    @Override
    public <R> R accept( NodeVisitor<R> visitor ) {
        return null;
    }

}
