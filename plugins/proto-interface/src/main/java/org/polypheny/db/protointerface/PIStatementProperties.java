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

package org.polypheny.db.protointerface;

import lombok.Getter;
import org.polypheny.db.protointerface.proto.StatementProperties;

public class PIStatementProperties {
    private static final int UNLIMITED = -1;
    private static final int DEFAULT_FETCH_SIZE = 100;

    public PIStatementProperties(StatementProperties statementProperties) {
        update(statementProperties);
    }

    private PIStatementProperties() {
    }


    public static PIStatementProperties getDefaultInstance() {
        PIStatementProperties defaults = new PIStatementProperties();
        defaults.updateBehaviour = StatementProperties.ResultOperations.READ_ONLY; // currently not in use
        defaults.fetchSize = DEFAULT_FETCH_SIZE;
        defaults.reverseFetch = false; // currently not in use
        defaults.maxTotalFetchSize = UNLIMITED; // currently not in use
        defaults.doesEscapeProcessing = false; // currently not in use
        defaults.isPoolable = false; // currently not in use
        return defaults;
    }

    @Getter
    private StatementProperties.ResultOperations updateBehaviour; // currently not in use
    @Getter
    private int fetchSize;
    @Getter
    private boolean reverseFetch; // currently not in use
    @Getter
    private long maxTotalFetchSize; // currently not in use
    @Getter
    private boolean doesEscapeProcessing; // currently not in use
    @Getter
    private boolean isPoolable; // currently not in use

    public void update(StatementProperties statementProperties) {
        this.updateBehaviour = statementProperties.getUpdateBehaviour();
        this.fetchSize = statementProperties.getFetchSize();
        this.reverseFetch = statementProperties.getReverseFetch();
        this.maxTotalFetchSize = statementProperties.getMaxTotalFetchSize();
        this.doesEscapeProcessing = statementProperties.getDoesEscapeProcessing();
        this.isPoolable = statementProperties.getIsPoolable();
    }
}
