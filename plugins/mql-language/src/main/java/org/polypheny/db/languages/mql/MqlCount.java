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


public class MqlCount extends MqlCollectionStatement implements MqlQueryStatement {

    @Getter
    private final boolean isEstimate;
    @Getter
    private final BsonDocument query;
    @Getter
    private final BsonDocument options;


    public MqlCount( ParserPos pos, String collection, String namespace, BsonDocument query, BsonDocument options ) {
        this( pos, collection, namespace, query, options, false );
    }


    public MqlCount( ParserPos pos, String collection, String namespace, BsonDocument query, BsonDocument options, boolean isEstimate ) {
        super( collection, namespace, pos );
        this.query = query;
        this.options = options;
        this.isEstimate = isEstimate;
    }


    @Override
    public Type getMqlKind() {
        return Type.COUNT;
    }


    @Override
    public @Nullable String getEntity() {
        return getCollection();
    }

}
