/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.workflow.jobs;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import io.javalin.http.HttpCode;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.jobs.JobManager.WorkflowJobException;

@Slf4j
public class CronScheduler {

    private static CronScheduler INSTANCE = null;
    private final Map<UUID, Pair<Cron, Runnable>> jobMap = new ConcurrentHashMap<>();
    private ScheduledExecutorService scheduler = null;
    private final CronParser parser = new CronParser( CronDefinitionBuilder.instanceDefinitionFor( CronType.UNIX ) );


    public static CronScheduler getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new CronScheduler();
        }
        return INSTANCE;
    }


    private CronScheduler() {
    }


    public synchronized void addJob( UUID jobId, String schedule, Runnable task ) throws IllegalArgumentException {
        Cron cron = parser.parse( schedule );
        jobMap.put( jobId, Pair.of( cron, task ) );
        if ( scheduler == null ) {
            log.warn( "Starting CronScheduler." );
            scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleAtFixedRate( this::checkAndRunJobs, 1, 1, TimeUnit.MINUTES );
        }
    }


    public synchronized void removeJob( UUID jobId ) {
        jobMap.remove( jobId );
        if ( jobMap.isEmpty() && scheduler != null ) {
            scheduler.shutdown();
            scheduler = null;
            log.warn( "CronScheduler has been shut down." );
        }
    }


    private void checkAndRunJobs() {
        ZonedDateTime now = ZonedDateTime.now(); // Get current timestamp
        log.warn( "Checking for jobs at {} with jobs {}", now, jobMap.keySet() );
        for ( Map.Entry<UUID, Pair<Cron, Runnable>> entry : jobMap.entrySet() ) {
            ExecutionTime executionTime = ExecutionTime.forCron( entry.getValue().left );
            executionTime.lastExecution( now ).ifPresent( lastRun -> {
                if ( lastRun.plusMinutes( 1 ).isAfter( now ) ) {
                    try {
                        entry.getValue().right.run(); // Run the job
                        log.warn( "Job {} has completed", entry.getKey() );
                    } catch ( Exception e ) {
                        log.error( "Error while running scheduled job for jobId {}", entry.getKey(), e );
                    }
                }
            } );
        }
    }


    public void validateCronExpression( String cronExpression ) throws WorkflowJobException {
        try {
            parser.parse( cronExpression ).validate();
        } catch ( IllegalArgumentException e ) {
            throw new WorkflowJobException( "Invalid Cron expression: " + e.getMessage(), HttpCode.BAD_REQUEST );
        }
    }

}
