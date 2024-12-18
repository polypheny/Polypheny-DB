/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.workflow.engine.monitoring;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ExecutionMonitor {

    private final List<ExecutionInfo> infos = new CopyOnWriteArrayList<>();
    private final Map<UUID, ExecutionInfo> activityToInfoMap = new ConcurrentHashMap<>();


    public void addInfo( ExecutionInfo info ) {
        infos.add( info );
        for ( UUID activityId : info.getActivities() ) {
            activityToInfoMap.put( activityId, info );
        }
    }


    public double getProgress( UUID activityId ) {
        ExecutionInfo info = activityToInfoMap.get( activityId );
        if ( info == null ) {
            return -1;
        }
        if ( info.usesCombinedProgress() ) {
            return info.getProgress();
        }
        return info.getProgress( activityId );
    }


    public Map<UUID, Double> getAllProgress() {
        Map<UUID, Double> progressMap = new HashMap<>();
        for ( ExecutionInfo info : infos ) {
            progressMap.putAll( info.getProgressSnapshot() );
        }
        return Collections.unmodifiableMap( progressMap );
    }

}
