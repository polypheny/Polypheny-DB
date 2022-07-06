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

package org.polypheny.db.processing;

import org.polypheny.db.PolyResult;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;

public class SqlProcessorFacade {
    private final SqlProcessorImpl sqlProcessor;

    public SqlProcessorFacade(SqlProcessorImpl sqlProcessor) {
        this.sqlProcessor = sqlProcessor;
    }

    public PolyResult runSql(String sql, Transaction transaction) {
        return getPolyResult(sql, transaction.createStatement());
    }
    public PolyResult runSql(String sql, Statement statement) {
        return getPolyResult(sql, statement);
    }

    private PolyResult getPolyResult(String sql, Statement statement) {
        Node parsed = sqlProcessor.parse(sql);
        PolyResult result;
        QueryParameters parameters = new QueryParameters(sql, Catalog.SchemaType.RELATIONAL);
        if (parsed.isA(Kind.DDL)) {
            result = sqlProcessor.prepareDdl(statement, parsed, parameters);
        } else {
            Pair<Node, AlgDataType> validated = sqlProcessor.validate(statement.getTransaction(), parsed, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean());
            AlgRoot logicalRoot = sqlProcessor.translate(statement, validated.left, parameters);
            result = statement.getQueryProcessor().prepareQuery(logicalRoot, true);
        }
        return result;
    }
}
