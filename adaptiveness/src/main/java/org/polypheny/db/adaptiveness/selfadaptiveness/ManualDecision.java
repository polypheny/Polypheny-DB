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
import lombok.Getter;
import org.polypheny.db.adaptiveness.policy.PoliceUtil.ClauseCategory;
import org.polypheny.db.adaptiveness.selfadaptiveness.SelfAdaptiveUtil.AdaptiveKind;
import org.polypheny.db.util.Pair;

/**
 * ManualDecision is used if the trigger is manually through a system change via policies.
 */
@Getter
public class ManualDecision<T> extends Decision {

    private final Pair<String, Action> key;
    private final ClauseCategory clauseCategory;


    // information needed to makeDecision
    private final Class<T> clazz;
    private final Action action;
    private final long nameSpaceId;
    private final long entityId;
    private final T preSelection;

    // can be used to say figure out if update should be triggered or not
    private long gainValue;


    public ManualDecision(
            Timestamp timestamp,
            ClauseCategory clauseCategory,
            String keyPart,
            AdaptiveKind adaptiveKind,
            Class<T> clazz,
            Action action,
            long nameSpaceId,
            long entityId,
            T preSelection ) {
        super( timestamp, adaptiveKind );
        this.key = new Pair<>( keyPart, action );
        this.clauseCategory = clauseCategory;
        this.clazz = clazz;
        this.action = action;
        this.nameSpaceId = nameSpaceId;
        this.entityId = entityId;
        this.preSelection = preSelection;
    }

}
