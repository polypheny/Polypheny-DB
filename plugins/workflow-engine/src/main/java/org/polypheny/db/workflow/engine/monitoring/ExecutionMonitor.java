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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.workflow.dag.Workflow;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorType;
import org.polypheny.db.workflow.models.responses.WsResponse;
import org.polypheny.db.workflow.models.responses.WsResponse.ProgressUpdateResponse;
import org.polypheny.db.workflow.models.responses.WsResponse.StateUpdateResponse;

public class ExecutionMonitor {

    private static final int UPDATE_PROGRESS_DELAY = 2000;
    private static final int STATE_UPDATE_DEFER_DELAY = 200;

    private final List<ExecutionInfo> infos = new CopyOnWriteArrayList<>();
    private final Map<UUID, ExecutionInfo> activityToInfoMap = new ConcurrentHashMap<>();
    private final StopWatch workflowDuration;

    private final Workflow workflow;
    @Getter
    private final UUID targetActivity;
    private final Consumer<WsResponse> callback; // used to send updates to clients
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile StateUpdateResponse scheduledUpdate; // Reduce number of state updates we send to the clients


    public ExecutionMonitor( Workflow workflow, @Nullable UUID targetActivity, @Nullable Consumer<WsResponse> callback ) {
        this.workflow = workflow;
        this.targetActivity = targetActivity;
        this.workflowDuration = StopWatch.createStarted();
        this.callback = callback;
        if ( callback != null ) {
            startPeriodicUpdates();
        }
    }


    private void startPeriodicUpdates() {
        scheduler.scheduleAtFixedRate( () -> {
            try {
                callback.accept( new ProgressUpdateResponse( null, getAllProgress() ) );
            } catch ( Exception e ) {
                e.printStackTrace(); // Log any exceptions
            }
        }, 500, UPDATE_PROGRESS_DELAY, TimeUnit.MILLISECONDS );
    }


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


    public Map<ExecutorType, Integer> getActivityCounts() {
        Map<ExecutorType, Integer> activityCounts = new HashMap<>();
        for ( ExecutionInfo info : infos ) {
            ExecutorType type = info.getExecutorType();
            activityCounts.put( type, activityCounts.getOrDefault( type, 0 ) + info.getActivities().size() );
        }
        return Collections.unmodifiableMap( activityCounts );
    }


    public void stop() {
        workflowDuration.stop();
        forwardStates();
        scheduler.shutdown();
    }


    public long getWorkflowDurationMillis() {
        return workflowDuration.getDuration().toMillis();
    }


    public synchronized void forwardStates() {
        if ( callback != null ) {
            boolean isScheduled = scheduledUpdate != null;
            scheduledUpdate = new StateUpdateResponse( null, workflow );
            if ( !isScheduled ) {
                scheduler.schedule( () -> {
                    callback.accept( scheduledUpdate );
                    scheduledUpdate = null;
                }, STATE_UPDATE_DEFER_DELAY, TimeUnit.MILLISECONDS );
            }
        }
    }

}
