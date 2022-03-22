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

package org.polypheny.db.adaptiveness.selfadaptiveness;

import java.sql.Timestamp;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.adaptiveness.policy.Clause.ClauseCategory;
import org.polypheny.db.adaptiveness.selfadaptiveness.SelfAdaptivAgent.DecisionStatus;
import org.polypheny.db.util.Pair;

@Getter
public class Decision <T>{

    private static final AtomicInteger atomicId = new AtomicInteger();

    /**
     * Unique id of Decision.
     */
    private final int id;
    private final Pair<Long, Action> key;
    private final Timestamp timestamp;
    private final ClauseCategory clauseCategory;
    @Setter
    private DecisionStatus decisionStatus;
    @Setter
    private WeightedList<?> weightedList;


    // information needed to makeDecision
    private final Class<T> clazz;
    private final Action action;
    private final long nameSpaceId;
    private final long entityId;
    private final T preSelection;

    // can be used to say figure out if update should be triggered or not
    private long gainValue;


    public Decision(

            Pair<Long, Action> key,
            Timestamp timestamp,
            ClauseCategory clauseCategory,
            Class<T> clazz,
            Action action,
            long nameSpaceId,
            long entityId,
            T preSelection ) {
        this.id = atomicId.getAndIncrement();
        this.key = key;
        this.timestamp = timestamp;
        this.clauseCategory = clauseCategory;
        this.clazz = clazz;
        this.action = action;
        this.nameSpaceId = nameSpaceId;
        this.entityId = entityId;
        this.preSelection = preSelection;
    }

}
