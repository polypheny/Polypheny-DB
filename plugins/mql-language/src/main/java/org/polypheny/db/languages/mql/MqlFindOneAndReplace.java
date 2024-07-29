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
import org.bson.BsonDocument;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.mql.Mql.Type;


public class MqlFindOneAndReplace extends MqlCollectionStatement implements MqlQueryStatement {

    @Getter
    private final BsonDocument query;
    @Getter
    private final BsonDocument replacement;
    @Getter
    private final BsonDocument projection;
    @Getter
    private final BsonDocument sort;
    @Getter
    private final BsonDocument upsert;


    public MqlFindOneAndReplace( ParserPos pos, String collection, String namespace, BsonDocument query, BsonDocument replacement, BsonDocument options ) {
        super( collection, namespace, pos );
        this.query = query;
        this.replacement = replacement;
        this.projection = getDocumentOrNull( options, "projection" );
        this.sort = getDocumentOrNull( options, "sort" );
        this.upsert = getDocumentOrNull( options, "upsert" );
    }


    @Override
    public Type getMqlKind() {
        return Type.FIND_REPLACE;
    }


    @Override
    public @Nullable String getEntity() {
        return getCollection();
    }

}
