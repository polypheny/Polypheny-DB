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
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.PIClient;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;

@Getter
@Slf4j
public abstract class PIStatement {

    protected final int id;
    protected final PIClient client;
    protected final StopWatch executionStopWatch;
    protected final QueryLanguage language;
    @Setter
    private ResultIterator iterator;
    protected LogicalNamespace namespace;


    protected PIStatement(
            int id, @NotNull PIClient client,
            @NotNull QueryLanguage language,
            @NotNull LogicalNamespace namespace ) {
        this.id = id;
        this.client = client;
        this.language = language;
        this.executionStopWatch = new StopWatch();
        this.namespace = namespace;
    }


    public void closeResults() {
        if ( iterator == null ) {
            return;
        }
        try {
            iterator.close();
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Closing of open result iterator failed" );
        }
    }


    public abstract PolyImplementation getImplementation();

    public abstract void setImplementation( PolyImplementation implementation );

    public abstract Statement getStatement();

    public abstract String getQuery();

    public abstract Transaction getTransaction();

}
