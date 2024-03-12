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

package org.polypheny.db.util.background;


import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.util.background.BackgroundTask.TaskDelayType;
import org.polypheny.db.util.background.BackgroundTask.TaskPriority;
import org.polypheny.db.util.background.BackgroundTask.TaskSchedulingType;

@Slf4j
class BackgroundTaskHandle implements Runnable {

    @Getter
    private final String id;
    @Getter
    private final BackgroundTask task;
    @Getter
    private final String description;
    @Getter
    private final TaskPriority priority;
    @Getter
    private final TaskSchedulingType schedulingType;

    private final StopWatch stopWatch = new StopWatch();
    private final MovingAverage avgExecTime = new MovingAverage( 100 );
    @Getter
    private long maxExecTime = 0L;

    private final ScheduledFuture<?> runner;


    public BackgroundTaskHandle( String id, BackgroundTask task, String description, TaskPriority priority, TaskSchedulingType schedulingType ) {
        this.id = id;
        this.task = task;
        this.description = description;
        this.priority = priority;
        this.schedulingType = schedulingType;

        // Schedule
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        if ( schedulingType.getDelayType() == TaskDelayType.FIXED ) {
            this.runner = exec.scheduleAtFixedRate( this, 0, schedulingType.getMillis(), TimeUnit.MILLISECONDS );
        } else if ( schedulingType.getDelayType() == TaskDelayType.DELAYED ) {
            this.runner = exec.scheduleWithFixedDelay( this, 0, schedulingType.getMillis(), TimeUnit.MILLISECONDS );
        } else {
            throw new GenericRuntimeException( "Unknown TaskDelayType: " + schedulingType.getDelayType().name() );
        }

    }


    public void stop() {
        if ( runner != null && !this.runner.isCancelled() ) {
            this.runner.cancel( false );
        }
    }


    public double getAverageExecutionTime() {
        return avgExecTime.getAverage();
    }


    @Override
    public void run() {
        try {
            stopWatch.reset();
            stopWatch.start();
            task.backgroundTask();
            stopWatch.stop();
            avgExecTime.add( stopWatch.getTime() );
            if ( maxExecTime < stopWatch.getTime() ) {
                maxExecTime = stopWatch.getTime();
            }
        } catch ( Exception e ) {

            log.error( "Caught exception in background task", e );
        }
    }


    // https://stackoverflow.com/a/19922501
    private static class MovingAverage {

        private final Queue<Long> window = new LinkedList<>();
        private final int period;
        private long sum = 0;


        public MovingAverage( int period ) {
            assert period > 0 : "Period must be a positive integer";
            this.period = period;
        }


        public void add( long x ) {
            sum += x;
            window.add( x );
            if ( window.size() > period ) {
                sum -= window.remove();
            }
        }


        public double getAverage() {
            if ( window.isEmpty() ) {
                return 0.0; // technically the average is undefined
            }
            return sum / (double) window.size();
        }

    }

}
