/*
 * Copyright 2019-2025 The Polypheny Project
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

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.mql.MqlNode;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.languages.mql.Mql.Type;

/**
 * MQL: db.alterCollection("<name>", { validator: { $jsonSchema: {...} }, validationAction: "warn|error|off" })
 * If 'validator' is omitted, the schema is dropped (collection becomes schemaless).
 */
public class MqlAlterCollection extends MqlNode implements ExecutableStatement {

    private final String name;
    private final BsonDocument options;

    public MqlAlterCollection(ParserPos pos, String name, String namespace, BsonDocument options) {
        super(pos, namespace);
        this.name = name;
        this.options = options;
    }

    @Override
    public Type getMqlKind() {
        return Type.ALTER_COLLECTION;
    }

    @Override
    public @Nullable String getEntity() {
        return name;
    }

    @Override
    public void execute(Context context, Statement statement, ParsedQueryContext parsedQueryContext) {
        long namespaceId = parsedQueryContext.getNamespaceId();
        String json = options != null ? options.toJson() : "{}";
        DdlManager.getInstance().alterCollection(namespaceId, name, statement, json);
    }

    @Override
    public String toString() {
        return "MqlAlterCollection{name='" + name + "'}";
    }
}
