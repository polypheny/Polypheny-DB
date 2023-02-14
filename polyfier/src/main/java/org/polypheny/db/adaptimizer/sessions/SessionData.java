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

package org.polypheny.db.adaptimizer.sessions;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.adaptimizer.exceptions.AdaptiveOptException;
import org.polypheny.db.adaptimizer.rndqueries.QueryTemplate;
import org.polypheny.db.util.Pair;

/**
 * Stores session data accessed by the {@link OptSession} and acts as communication link to the {@link SessionData#sessionThread} for
 * other Threads and the {@link SessionMonitor}. Further, keeps track of Errors, Exceptions and Execution Times
 * ({@link SessionData#errorList}, {@link SessionData#exceptionList}, {@link SessionData#totalTimes}), where the Execution Times include
 * error and exception resolution as well as querying added to the routing and optimization process.
 */
@Slf4j
public class SessionData {

    @Getter(AccessLevel.MODULE)
    private final QueryTemplate template;

    @Getter(AccessLevel.PUBLIC)
    private final long initialSeed;

    @Getter(AccessLevel.PUBLIC)
    private final String sessionId;

    @Getter(AccessLevel.PUBLIC)
    @Setter(AccessLevel.PUBLIC)
    private Thread sessionThread;

    @Getter(AccessLevel.PUBLIC)
    @Setter(AccessLevel.PUBLIC)
    private OptSession optSession;

    private final StopWatch stopWatch;
    private long runtime;

    @Getter(AccessLevel.PUBLIC)
    private final int orderedQueries;
    private int queriesExecuted;

    private final LinkedList<Pair<Long, Throwable>> errorList;
    private int errors;
    private int seedSwitches;

    private final LinkedList<Pair<Long, Throwable>> exceptionList;
    private int exceptions;

    private boolean started;
    private boolean finished;
    private boolean interrupted;
    private boolean markedForInterruption;

    private int executions;

    private final ArrayList<Long> totalTimes;
    private final ArrayList<Long> exceptionTimes;
    private final ArrayList<Long> errorTimes;
    private final ArrayList<Long> successTimes;

    public int earlyFaults;

    public int xMax;
    public int xMin;
    public int xAddCalls;

    ArrayList<Integer> xAxis;

    ArrayList<String> informationPlans;

    ArrayList<Long> successTimeMonitor;
    ArrayList<Long> errorTimeMonitor;
    ArrayList<Long> exceptionTimeMonitor;
    ArrayList<Long> totalTimeMonitor;

    public SessionData(final String sessionId,
            final QueryTemplate template, Thread sessionThread, OptSession optSession, int orderedQueries ) {

        this.template = template;
        this.initialSeed = template.getSeed();
        this.sessionId = sessionId;

        this.sessionThread = sessionThread;
        this.optSession = optSession;

        this.stopWatch = new StopWatch();
        this.runtime = 0;

        this.orderedQueries = orderedQueries;
        this.queriesExecuted = 0;

        this.started = false;
        this.finished = false;
        this.interrupted = false;
        this.markedForInterruption = false;

        this.seedSwitches = 0;

        this.exceptionList = new LinkedList<>();
        this.exceptions = 0;

        this.errorList = new LinkedList<>();
        this.errors = 0;

        this.executions = 0;

        // Execution-Time Monitoring
        this.totalTimes = new ArrayList<>();
        this.exceptionTimes = new ArrayList<>();
        this.errorTimes = new ArrayList<>();
        this.successTimes = new ArrayList<>();

        this.xMin = 0;
        this.xMax = 0;
        this.xAddCalls = 0;

        this.xAxis = new ArrayList<>();

        this.successTimeMonitor = new ArrayList<>();
        this.errorTimeMonitor = new ArrayList<>();
        this.exceptionTimeMonitor = new ArrayList<>();
        this.totalTimeMonitor = new ArrayList<>();

        informationPlans = new ArrayList<>();
    }

    public synchronized void addQueryExecutionTime(
            ExecutionResult result, long seed, long time, Throwable ex ) {
        switch ( result ) {
            case ERROR:
                this.errors++;
                this.errorTimes.add( time );
                this.errorList.add( new Pair<>( seed, ex ) );
                break;
            case EXCEPTION:
                this.exceptions++;
                this.exceptionTimes.add( time );
                this.exceptionList.add( new Pair<>( seed, ex ) );
                break;
            case SUCCESS:
                this.queriesExecuted++;
                this.successTimes.add( time );
                break;
        }
        this.executions++;
        this.totalTimes.add( time );
    }

    public synchronized void markForInterruption() {
        this.markedForInterruption = true;
    }

    public synchronized boolean isMarkedForInterruption() {
        return this.markedForInterruption;
    }

    public synchronized int getTotalExecutions() {
        return this.executions;
    }

    public synchronized void interrupt() {
        this.stopWatch.stop();
        this.runtime = this.stopWatch.getTime( TimeUnit.SECONDS );
        this.sessionThread.interrupt();
        this.interrupted = true;
    }

    public synchronized void finish() {
        this.stopWatch.stop();
        this.runtime = this.stopWatch.getTime( TimeUnit.SECONDS );
        this.finished = true;
    }

    public synchronized boolean isFinished() {
        return this.finished;
    }

    public synchronized void start() {
        this.stopWatch.start();
        this.started = true;
        this.sessionThread.start();
    }

    public synchronized void continueSession() {
        this.sessionThread.start();
    }

    public synchronized boolean isStarted() {
        return this.started;
    }

    public synchronized long getCurrentTime() {
        return this.stopWatch.getTime( TimeUnit.SECONDS );
    }

    public synchronized void incrementSeedSwitches() {
        this.seedSwitches += 1;
    }

    public synchronized int getSeedSwitches() {
        return this.seedSwitches;
    }

    public synchronized long getFinalRuntime() {
        if ( ! ( this.finished || this.interrupted ) ) {
            throw new AdaptiveOptException( "Trying to get final runtime of incomplete session.", new NullPointerException() );
        }
        return this.runtime;
    }

    public synchronized int getQueriesExecuted() {
        return queriesExecuted;
    }

    public synchronized List<Pair<Long, Throwable>> getErrors() {
        return this.errorList;
    }

    public synchronized int getNumberOfErrors() {
        return this.errors;
    }

    public synchronized int getNumberOfExceptions() {
        return this.exceptions;
    }

    /**
     * Gets the current measurement X-Axis as a String array.
     */
    public synchronized String[] getXAxis() {
        return xAxis.subList( xAxis.size() - ( xMax - xMin ), xAxis.size() - 1 ).stream().map( Object::toString ).toArray( String[]::new );
    }

    /**
     * Adds a time-step to the measurements X-Axis, cumulatively summing with more values.
     */
    public synchronized void xAdd( int val ) {
        xMax += 1;
        if ( xMax > 10 ) {
            xMin += 1;
        }
        if ( xAxis.isEmpty() ) {
            xAxis.add( val );
        } else {
            xAxis.add( xAxis.get( xAxis.size() - 1 ) + val );
        }
        updateSuccessTime();
        updateExceptionTime();
        updateErrorTime();
        updateTotalTime();
    }

    public synchronized void updateSuccessTime() {
        successTimeMonitor.add( rAvgTime( this.successTimes ) );
    }

    public synchronized void updateErrorTime() {
        errorTimeMonitor.add( rAvgTime( this.errorTimes ) );
    }

    public synchronized void updateExceptionTime() {
        exceptionTimeMonitor.add( rAvgTime( this.exceptionTimes ) );
    }

    public synchronized void updateTotalTime() {
        totalTimeMonitor.add( rAvgTime( this.totalTimes ) );
    }

    public synchronized Long[] getSTm() {
        return toArray( successTimeMonitor );
    }

    public synchronized Long[] getErTm() {
        return toArray( errorTimeMonitor );
    }

    public synchronized Long[] getExTm() {
        return toArray( exceptionTimeMonitor );
    }

    public synchronized Long[] getToTm() {
        return toArray( totalTimeMonitor );
    }

    private synchronized Long[] toArray( ArrayList<Long> xs ) {
        if ( xs.isEmpty() ) {
            throw new IllegalArgumentException( "List can not be empty..." );
        }
        return xs.subList( xs.size() - ( xMax - xMin ), xs.size() - 1 ).toArray( Long[]::new );
    }

    private synchronized long rAvgTime( ArrayList<Long> xs ) {
        if ( xs.isEmpty() ) {
            return 0;
        }
        // ?
        return xs.subList( Math.max( xs.size() - Math.max( xMax - xMin, 1 ), 0 ), xs.size() - 1 ).stream().mapToLong( Long::longValue ).sum() / ( xMax - xMin );
    }

}
