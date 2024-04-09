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

import lombok.Getter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.mql.Mql.Type;


public class MqlFindAndModify extends MqlCollectionStatement implements MqlQueryStatement {

    @Getter
    private final BsonDocument query;
    @Getter
    private final BsonDocument sort;
    @Getter
    private final boolean remove;
    @Getter
    private final BsonDocument update;
    @Getter
    private final boolean newKey;
    @Getter
    private final BsonDocument fields;
    @Getter
    private final boolean upsert;
    @Getter
    private final boolean bypassDocumentValidation;
    @Getter
    private final BsonDocument collation;
    @Getter
    private final BsonArray arrayFilters;
    @Getter
    private final BsonDocument let;


    public MqlFindAndModify( ParserPos pos, String collection, String namespace, BsonDocument document ) {
        super( collection, namespace, pos );
        this.query = getDocumentOrNull( document, "query" );
        this.sort = getDocumentOrNull( document, "sort" );
        this.remove = getBoolean( document, "remove" );
        this.update = getDocumentOrNull( document, "update" );
        this.newKey = getBoolean( document, "new" );
        this.fields = getDocumentOrNull( document, "fields" );
        this.upsert = getBoolean( document, "upsert" );
        this.bypassDocumentValidation = getBoolean( document, "bypassDocumentValidation" );
        //this.writeConcern
        this.collation = getDocumentOrNull( document, "collation" );
        this.arrayFilters = getArrayOrNull( document, "arrayFilters" );
        this.let = getDocumentOrNull( document, "document" );

    }


    @Override
    public Type getMqlKind() {
        return Type.FIND_MODIFY;
    }


    @Override
    public @Nullable String getEntity() {
        return getCollection();
    }

}
