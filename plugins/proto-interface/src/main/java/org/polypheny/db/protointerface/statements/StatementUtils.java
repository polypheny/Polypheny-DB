/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.protointerface.statements;

import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.protointerface.proto.ColumnMeta;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.relational.RelationalMetaRetriever;
import org.polypheny.db.protointerface.utils.ProtoUtils;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Objects;

public class StatementUtils {

    private static void startOrResumeStopwatch(StopWatch stopWatch) {
        if (stopWatch.isSuspended()) {
            stopWatch.resume();
            return;
        }
        if (stopWatch.isStopped()) {
            stopWatch.start();
        }
    }

    public static Frame relationalFetch(PIStatement piStatement, long offset) {
        int fetchSize = piStatement.getProperties().getFetchSize();
        StopWatch executionStopWatch = piStatement.getExecutionStopWatch();
        PolyImplementation<PolyValue> implementation = piStatement.getImplementation();
        startOrResumeStopwatch(executionStopWatch);
        // TODO: implement continuation of fetching
        List<List<PolyValue>> rows = implementation.getRows(implementation.getStatement(), fetchSize);
        executionStopWatch.suspend();
        boolean isDone = fetchSize == 0 || Objects.requireNonNull(rows).size() < fetchSize;
        if (isDone) {
            executionStopWatch.stop();
            implementation.getExecutionTimeMonitor().setExecutionTime(executionStopWatch.getNanoTime());
        }
        List<ColumnMeta> columnMetas = RelationalMetaRetriever.retrieveColumnMetas(implementation);
        return ProtoUtils.buildRelationalFrame(offset, isDone, rows, columnMetas);
    }

    public static Frame graphFetch(PIStatement statement, long offset) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Graph Fetching is not yet implmented.");
    }


    public static Frame documentFetch(PIStatement statement, long offset) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Doument fetching is no yet implemented.");
    }


    public static void execute(PIStatement piStatement) throws Exception {
        PolyImplementation<PolyValue> implementation;

        String query = piStatement.getQuery();
        QueryLanguage queryLanguage = piStatement.getLanguage();
        Statement statement = piStatement.getStatement();

        Processor queryProcessor = statement.getTransaction().getProcessor(queryLanguage);
        Node parsedStatement = queryProcessor.parse(query).get(0);
        if (parsedStatement.isA(Kind.DDL)) {
            implementation = queryProcessor.prepareDdl(statement, parsedStatement,
                    new QueryParameters(query, queryLanguage.getNamespaceType()));
        } else {
            Pair<Node, AlgDataType> validated = queryProcessor.validate(piStatement.getTransaction(),
                    parsedStatement, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean());
            AlgRoot logicalRoot = queryProcessor.translate(statement, validated.left, null);
            AlgDataType parameterRowType = queryProcessor.getParameterRowType(validated.left);
            implementation = statement.getQueryProcessor().prepareQuery(logicalRoot, parameterRowType, true);
        }
        piStatement.setImplementation(implementation);
    }

    public static void prepare(PIPreparedStatement piStatement) {
        Transaction transaction = piStatement.getClient().getCurrentOrCreateNewTransaction();
        String query = piStatement.getQuery();
        QueryLanguage queryLanguage = piStatement.getLanguage();

        Processor queryProcessor = transaction.getProcessor(queryLanguage);
        Node parsed = queryProcessor.parse(query).get(0);
        // It is important not to add default values for missing fields in insert statements. If we did this, the
        // JDBC driver would expect more parameter fields than there actually are in the query.
        Pair<Node, AlgDataType> validated = queryProcessor.validate(transaction, parsed, false);
        AlgDataType parameterRowType = queryProcessor.getParameterRowType(validated.left);
        piStatement.setParameterMetas(RelationalMetaRetriever.retrieveParameterMetas(parameterRowType));
    }
}
