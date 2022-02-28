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

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import lombok.Getter;
import org.checkerframework.checker.units.qual.C;
import org.polypheny.db.policies.policy.Policy.Target;

@Getter
public abstract class Clause {

    private static final AtomicInteger atomicId = new AtomicInteger();
    /**
     * Name of clause.
     */

    private final ClauseName clauseName;

    /**
     * Unique id of clause.
     */
    private final int id;

    private final boolean isDefault;

    private final ClauseType clauseType;

    private final Category category;

    private final String description;

    private final List<Target> possibleTargets;

    private final HashMap<AffectedOperations, Function<List<Object>, List<Object>>> decide;

    //first Value is its own setting, second value is interfering with it
    private final HashMap<Clause, Clause> interfering;


    protected Clause( ClauseName clauseName, boolean isDefault, ClauseType clauseType, Category category, List<Target> possibleTargets, String description, HashMap<AffectedOperations, Function<List<Object>, List<Object>>> decide, HashMap<Clause, Clause> interfering ) {
        this.id = atomicId.getAndIncrement();
        this.clauseName = clauseName;
        this.isDefault = isDefault;
        this.clauseType = clauseType;
        this.category = category;
        this.description = description;
        this.possibleTargets = possibleTargets;
        this.decide = decide;
        this.interfering = interfering;

    }


    public boolean isA( ClauseType clauseType ) {
        return this.getClauseType() == clauseType;
    }


    public abstract <T extends Clause> BooleanClause copy();


    public <T extends Clause> T copyClause() {
        T clause = (T) copy();
        assert clause.getId() != this.id;
        return clause;
    }


    public abstract boolean compareClause( Clause clauseAddition );


    /**
     * Different Categories are used to describe the different policies used in Polypheny
     */
    public enum Category {
        STORE
    }

    public enum AffectedOperations {
        STORE
    }

    public enum ClauseName {
        FULLY_PERSISTENT, ONLY_EMBEDDED, ONLY_DOCKER, PERSISTENT

    }


    public enum ClauseType {
        BOOLEAN, NUMBER
    }

}
