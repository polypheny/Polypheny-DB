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
import org.polypheny.db.protointerface.proto.Holdability;
import org.polypheny.db.protointerface.proto.StatementProperties;

public class InterfaceStatementProperties {
    public InterfaceStatementProperties(StatementProperties statementProperties) {
        update(statementProperties);
    }

    @Getter
    private StatementProperties.ResultOperations updateBehaviour;
    @Getter
    private Holdability holdability;
    @Getter
    private int fetch_size;
    @Getter
    private boolean reverse_fetch;
    @Getter
    private long max_total_fetch_size;
    @Getter
    private boolean does_escape_processing;
    @Getter
    private boolean is_poolable;

    public void update(StatementProperties statementProperties) {
        this.updateBehaviour = statementProperties.getUpdateBehaviour();
        this.holdability = statementProperties.getHoldability();
        this.fetch_size = statementProperties.getFetchSize();
        this.reverse_fetch = statementProperties.getReverseFetch();
        this.max_total_fetch_size = statementProperties.getMaxTotalFetchSize();
        this.does_escape_processing = statementProperties.getDoesEscapeProcessing();
        this.is_poolable = statementProperties.getIsPoolable();
    }
}
