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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.polypheny.db.policies.policy.Policy.Target;

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

    @Getter
    private final List<Target> possibleTargets;


    protected Clause( ClauseName clauseName, boolean isDefault, ClauseType clauseType, Category category, List<Target> possibleTargets, String description ) {
        this.id = atomicId.getAndIncrement();
        this.clauseName = clauseName;
        this.isDefault = isDefault;
        this.clauseType = clauseType;
        this.category = category;
        this.description = description;
        this.possibleTargets = possibleTargets;

    }


    /**
     * Different Categories are used to describe the different policies used in Polypheny
     */
    public enum Category {
        STORE
    }


    public enum ClauseName {
        FULLY_PERSISTENT, ONLY_EMBEDDED, ONLY_DOCKER, PERSISTENT

    }

    public enum ClauseType {
        BOOLEAN, NUMBER
    }

}
