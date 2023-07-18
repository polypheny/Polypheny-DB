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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.linq4j.Enumerable;
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
import org.polypheny.db.protointerface.PIClient;
import org.polypheny.db.protointerface.PIStatementProperties;
import org.polypheny.db.protointerface.PIServiceException;
import org.polypheny.db.protointerface.proto.ColumnMeta;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.StatementProperties;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.relational.RelationalMetaRetriever;
import org.polypheny.db.protointerface.utils.ProtoUtils;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.LimitIterator;
import org.polypheny.db.util.Pair;

import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
public abstract class PIStatement {


    @Getter
    protected final int statementId;
    @Getter
    protected final PIClient protoInterfaceClient;
    protected final PIStatementProperties properties;
    protected final StopWatch executionStopWatch;
    protected final QueryLanguage queryLanguage;
    protected String query;
    boolean allowOverwrite;
    protected PolyImplementation<PolyValue> currentImplementation;
    protected Iterator<PolyValue> resultIterator;


    protected PIStatement(Builder builder) {
        if (builder.query == null) {
            throw new NullPointerException("statement must not be null.");
        }
        if (builder.protoInterfaceClient == null) {
            throw new NullPointerException("proto interface client must not be null.");
        }
        if (builder.queryLanguage == null) {
            throw new NullPointerException("query language must not be null.");
        }
        this.statementId = builder.statementId;
        this.protoInterfaceClient = builder.protoInterfaceClient;
        this.queryLanguage = builder.queryLanguage;
        this.query = builder.query;
        this.executionStopWatch = new StopWatch();
        this.properties = builder.properties;
        this.allowOverwrite = true;
    }


    protected void overwriteQuery(String query) {
        if (!allowOverwrite) {
            throw new PIServiceException("Query overwrite not permitted after execution of statement");
        }
        this.query = query;
    }


    public abstract StatementResult execute() throws Exception;


    protected StatementResult execute(Statement statement) throws Exception {
        this.allowOverwrite = false;
        Processor queryProcessor = statement.getTransaction().getProcessor(queryLanguage);
        Node parsedStatement = queryProcessor.parse(query).get(0);
        if (parsedStatement.isA(Kind.DDL)) {
            currentImplementation = queryProcessor.prepareDdl(statement, parsedStatement,
                    new QueryParameters(query, queryLanguage.getNamespaceType()));
        } else {
            Pair<Node, AlgDataType> validated = queryProcessor.validate(protoInterfaceClient.getCurrentTransaction(),
                    parsedStatement, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean());
            AlgRoot logicalRoot = queryProcessor.translate(statement, validated.left, null);
            AlgDataType parameterRowType = queryProcessor.getParameterRowType(validated.left);
            currentImplementation = statement.getQueryProcessor().prepareQuery(logicalRoot, parameterRowType, true);
        }

        StatementResult.Builder resultBuilder = StatementResult.newBuilder();
        if (Kind.DDL.contains(currentImplementation.getKind())) {
            resultBuilder.setScalar(1);
            commitIfAuto();
            return resultBuilder.build();
        }
        if (Kind.DML.contains(currentImplementation.getKind())) {
            resultBuilder.setScalar(currentImplementation.getRowsChanged(statement));
            commitIfAuto();
            return resultBuilder.build();
        }

        commitIfAuto();
        resultBuilder.setFrame(fetch(0));

        return resultBuilder.build();
    }


    protected void commitIfAuto() throws IllegalArgumentException {
        //TODO TH: debug why not updated
        if (!protoInterfaceClient.isAutocommit()) {
            return;
        }
        commitElseRollback();
    }



    public Frame fetch(long offset) throws Exception {
        switch (queryLanguage.getNamespaceType()) {
            case RELATIONAL:
                return relationalFetch(offset);
            case GRAPH:
                return graphFetch(offset);
            case DOCUMENT:
                return documentFetch(offset);
        }
        throw new PIServiceException("Should never be thrown.");
    }


    public Frame relationalFetch(long offset) {
        if (currentImplementation == null) {
            throw new PIServiceException("Can't fetch frames of an unexecuted statement");
        }
        synchronized (protoInterfaceClient) {
            if (log.isTraceEnabled()) {
                log.trace("fetch(long {}, int {} )", offset, properties.getFetchSize());
            }
            startOrResumeStopwatch();
            List<List<PolyValue>> rows = getRows(LimitIterator.of(getOrCreateIterator(), properties.getFetchSize()));
            executionStopWatch.suspend();
            boolean isDone = properties.getFetchSize() == 0 || rows.size() < properties.getFetchSize();
            if (isDone) {
                executionStopWatch.stop();
                currentImplementation.getExecutionTimeMonitor().setExecutionTime(executionStopWatch.getNanoTime());
            }
            List<ColumnMeta> columnMetas = RelationalMetaRetriever.retrieveColumnMetas(currentImplementation);
            return ProtoUtils.buildRelationalFrame(offset, isDone, rows, columnMetas);
        }
    }


    private List<List<PolyValue>> getRows(Iterator sectionIterator) {
        return (List<List<PolyValue>>) MetaImpl.collect(currentImplementation.getCursorFactory(), sectionIterator, new ArrayList<>());
    }


    private Frame graphFetch(long offset) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Graph Fetching is not yet implmented.");
    }


    private Frame documentFetch(long offset) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Doument fetching is no yet implemented.");
    }


    protected Iterator<PolyValue> getOrCreateIterator() {
        if (resultIterator != null) {
            return resultIterator;
        }
        Statement statement = currentImplementation.getStatement();
        final Enumerable<PolyValue> enumerable = currentImplementation.enumerable(statement.getDataContext());
        resultIterator = enumerable.iterator();
        return resultIterator;
    }


    protected void startOrResumeStopwatch() {
        if (executionStopWatch.isSuspended()) {
            executionStopWatch.resume();
            return;
        }
        if (executionStopWatch.isStopped()) {
            executionStopWatch.start();
        }
    }


    protected void commitElseRollback() {
        try {
            protoInterfaceClient.commitCurrentTransaction();
        } catch (Exception e) {
            protoInterfaceClient.rollbackCurrentTransaction();
        }
    }

    public void updateProperties(StatementProperties statementProperties) {
        properties.update(statementProperties);
    }

    static abstract class Builder {

        protected int statementId;
        protected PIClient protoInterfaceClient;
        protected QueryLanguage queryLanguage;
        protected String query;
        protected PIStatementProperties properties;


        protected Builder() {
        }
        
    }


}
