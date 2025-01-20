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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorType;
import org.polypheny.db.workflow.models.ExecutionInfoModel;

public class ExecutionInfo {

    public static final int MIN_LOG_CAPACITY = 10;

    private final StopWatch totalDuration;
    private final Map<ExecutionState, StopWatch> durations = new HashMap<>();
    @Getter
    private final Set<UUID> activities;
    @Getter
    private final ExecutorType executorType;
    @Getter
    private ExecutionState state;
    @Getter
    @Setter
    private boolean isSuccess;
    private final Map<UUID, Double> progressMap = new ConcurrentHashMap<>();
    private double combinedProgress; // only used by FusionExecutor, since we cannot specify the progress of individual activities
    @Getter
    private long tuplesWritten = -1;

    private final CircularFifoQueue<String> log;


    public ExecutionInfo( Set<UUID> activities, ExecutorType executorType, int logCapacity ) {
        this.activities = Collections.unmodifiableSet( activities );
        this.executorType = executorType;
        activities.forEach( n -> progressMap.put( n, 0. ) );

        this.state = ExecutionState.SUBMITTED;
        this.totalDuration = StopWatch.createStarted();
        this.durations.put( state, StopWatch.createStarted() );
        this.log = new CircularFifoQueue<>( Math.min( logCapacity, MIN_LOG_CAPACITY ) );
    }


    public void setState( ExecutionState state ) {
        assert !durations.containsKey( state ) && state.ordinal() > this.state.ordinal();

        durations.get( this.state ).stop();
        this.state = state;

        if ( state == ExecutionState.DONE ) {
            totalDuration.stop();
        } else {
            durations.put( state, StopWatch.createStarted() );
        }
    }


    public void setTuplesWritten( long tuplesWritten ) {
        if ( this.tuplesWritten >= 0 ) {
            return;
        }
        this.tuplesWritten = tuplesWritten;
    }


    public double getProgress() {
        if ( usesCombinedProgress() ) {
            return combinedProgress;
        }

        double totalProgress = 0;
        for ( double progress : progressMap.values() ) {
            totalProgress += progress;
        }
        return totalProgress / activities.size();
    }


    public double getProgress( UUID activityId ) {
        assert !usesCombinedProgress();
        return progressMap.get( activityId );
    }


    public Map<UUID, Double> getProgressSnapshot() {
        if ( usesCombinedProgress() ) {
            double progress = combinedProgress; // copy value
            Map<UUID, Double> map = new HashMap<>();
            activities.forEach( n -> map.put( n, progress ) );
            return Collections.unmodifiableMap( map );
        } else {
            return Map.copyOf( progressMap );
        }
    }


    public void setProgress( UUID activityId, double progress ) {
        assert activities.contains( activityId ) && !usesCombinedProgress();

        progress = Math.min( 1, progress );
        if ( progress > progressMap.get( activityId ) ) {
            progressMap.put( activityId, progress );
        }
    }


    public void setProgress( double progress ) {
        assert usesCombinedProgress() : "Setting the progress of all activities at once is only supported by the FusionExecutor";
        progress = Math.min( 1, progress );

        if ( progress > combinedProgress ) {
            combinedProgress = progress;
        }
    }


    public long getDurationMillis() {
        return totalDuration.getDuration().toMillis();
    }


    public long getDurationMillis( ExecutionState state ) {
        assert state != ExecutionState.DONE;
        StopWatch stopWatch = durations.get( state );
        if ( stopWatch == null ) {
            return -1;
        }
        return stopWatch.getDuration().toMillis();
    }


    public boolean isDone() {
        return state == ExecutionState.DONE;
    }


    public boolean usesCombinedProgress() {
        return executorType == ExecutorType.FUSION;
    }


    public synchronized void appendLog( UUID activityId, LogLevel level, String message ) {
        String logMessage = activityId + "|" + level + "|" + getDurationMillis( ExecutionState.EXECUTING ) + "|" + message;
        log.add( logMessage );
    }


    public synchronized ExecutionInfoModel toModel( boolean includeLog ) {
        return new ExecutionInfoModel(
                totalDuration.getStartInstant().toString(),
                getDurationMillis(),
                Arrays.stream( ExecutionState.values() )
                        .filter( s -> s != ExecutionState.DONE )
                        .collect( Collectors.toMap( s -> s, this::getDurationMillis ) ),
                new ArrayList<>( activities ),
                executorType,
                state,
                isSuccess,
                tuplesWritten,
                includeLog ? List.copyOf( log ) : null
        );
    }


    @Override
    public String toString() {
        // This does no longer capture the ExecutionInfo in its entirety
        StringJoiner dJoiner = new StringJoiner( "\n", "\n", "" );
        for ( ExecutionState state : ExecutionState.values() ) {
            if ( state == ExecutionState.DONE ) {
                continue;
            }
            dJoiner.add( "\t\t" + state + ": " + getDurationMillis( state ) );
        }

        String individualProgress = "";
        if ( !usesCombinedProgress() ) {
            StringJoiner pJoiner = new StringJoiner( "\n", "\n", "" );
            for ( UUID n : activities ) {
                pJoiner.add( "\t\t" + n + ": " + getProgress( n ) );
            }
            individualProgress = pJoiner.toString();
        }

        return "ExecutionInfo:" +
                "\n\texecutorType=" + executorType +
                "\n\tstate=" + state +
                "\n\ttotalDuration=" + getDurationMillis() +
                dJoiner +
                "\n\ttotalProgress=" + getProgress() +
                individualProgress;
    }


    public enum ExecutionState {
        SUBMITTED,
        EXECUTING,
        AWAIT_PROCESSING,
        PROCESSING_RESULT,
        DONE
    }


    public enum LogLevel {
        INFO,
        WARN,
        ERROR
    }

}
