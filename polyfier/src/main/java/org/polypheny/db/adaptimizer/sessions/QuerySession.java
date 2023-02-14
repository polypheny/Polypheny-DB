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

package org.polypheny.db.adaptimizer.sessions;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.adaptimizer.rndqueries.AbstractQuerySupplier;
import org.polypheny.db.adaptimizer.rndqueries.QueryService;

@Getter(AccessLevel.PUBLIC)
public abstract class QuerySession implements OptSession {

    protected final AbstractQuerySupplier querySupplier;
    protected final QueryService queryService;

    @Setter(AccessLevel.PUBLIC)
    protected SessionData sessionData;

    public QuerySession(
            AbstractQuerySupplier querySupplier
    ) {
        this.querySupplier = querySupplier;
        this.queryService = new QueryService();
    }

}
