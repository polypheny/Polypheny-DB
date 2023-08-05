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

import java.util.Iterator;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.PIClient;
import org.polypheny.db.protointerface.PIStatementProperties;
import org.polypheny.db.protointerface.proto.StatementProperties;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;

@Slf4j
public abstract class PIStatement {

    @Getter
    protected final int id;
    @Getter
    protected final PIClient client;
    @Getter
    protected final PIStatementProperties properties;
    @Getter
    protected final StopWatch executionStopWatch;
    @Getter
    protected final QueryLanguage language;
    @Getter
    @Setter
    private Iterator<PolyValue> iterator;
    @Getter
    protected LogicalNamespace namespace;


    protected PIStatement(
            int id, @NotNull PIClient client,
            @NotNull PIStatementProperties properties,
            @NotNull QueryLanguage language,
            @NotNull LogicalNamespace namespace) {
        this.id = id;
        this.client = client;
        this.properties = properties;
        this.language = language;
        this.executionStopWatch = new StopWatch();
        this.namespace = namespace;
    }


    public abstract PolyImplementation<PolyValue> getImplementation();

    public abstract void setImplementation( PolyImplementation<PolyValue> implementation );

    public abstract Statement getStatement();

    public abstract String getQuery();

    public Transaction getTransaction() {
        return client.getCurrentTransaction();
    }


    public void updateProperties( StatementProperties statementProperties ) {
        properties.update( statementProperties );
    }

}
