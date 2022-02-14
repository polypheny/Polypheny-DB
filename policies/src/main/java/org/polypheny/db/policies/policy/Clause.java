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

package org.polypheny.db.policies.policy;

import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;

public abstract class Clause {

    private static final AtomicInteger atomicId = new AtomicInteger();
    /**
     * Name of clause.
     */
    @Getter
    private final ClauseName clauseName;

    /**
     * Unique id of clause.
     */
    @Getter
    private final int id;

    @Getter
    private final boolean isDefault;

    @Getter
    private final ClauseType clauseType;

    @Getter
    private final Category category;

    @Getter
    private final String description;


    protected Clause( ClauseName clauseName, boolean isDefault, ClauseType clauseType, Category category, String description ) {
        this.id = atomicId.getAndIncrement();
        this.clauseName = clauseName;
        this.isDefault = isDefault;
        this.clauseType = clauseType;
        this.category = category;
        this.description = description;

    }


    /**
     * Different Categories are used to describe the different policies used in Polypheny
     */
    public enum Category {
        AVAILABILITY, PERFORMANCE, REDUNDANCY, PERSISTENCY, TWO_PHASE_COMMIT
    }


    public enum ClauseName {
        FULLY_PERSISTENT, ONLY_EMBEDDED
    }

    public enum ClauseType {
        BOOLEAN, NUMBER
    }

}
