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
import org.polypheny.db.adaptiveness.selfadaptiveness.SelfAdaptiveUtil.AdaptiveKind;
import org.polypheny.db.adaptiveness.selfadaptiveness.SelfAdaptiveUtil.DecisionStatus;

@Getter
public class Decision {

    protected static final AtomicInteger atomicId = new AtomicInteger();
    /**
     * Unique id of Decision.
     */
    protected final int id;
    protected final Timestamp timestamp;
    protected final AdaptiveKind adaptiveKind;
    @Setter
    private DecisionStatus decisionStatus;
    @Setter
    private WeightedList<?> weightedList;


    public Decision( Timestamp timestamp, AdaptiveKind adaptiveKind ) {
        this.id = atomicId.getAndIncrement();
        this.timestamp = timestamp;
        this.adaptiveKind = adaptiveKind;
    }

}
