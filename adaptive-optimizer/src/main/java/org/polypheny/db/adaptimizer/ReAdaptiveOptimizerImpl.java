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

package org.polypheny.db.adaptimizer;


import java.util.HashMap;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adaptimizer.rndqueries.QuerySupplier;
import org.polypheny.db.adaptimizer.rndqueries.QueryTemplate;
import org.polypheny.db.adaptimizer.rndqueries.QueryUtil;
import org.polypheny.db.adaptimizer.rndqueries.RelQueryGenerator;
import org.polypheny.db.adaptimizer.sessions.OptSession;
import org.polypheny.db.adaptimizer.sessions.RelQuerySession;
import org.polypheny.db.adaptimizer.sessions.SessionData;
import org.polypheny.db.adaptimizer.sessions.SessionMonitor;
import org.polypheny.db.adaptimizer.sessions.SessionUtil;


/**
 * The {@link ReAdaptiveOptimizerImpl} is the base class of the Adaptive Optimizer
 * and exposes functions for the UI.
 */
@Slf4j
public class ReAdaptiveOptimizerImpl extends AdaptiveOptimizerImpl {
    private static final int DEFAULT_GEN_NR = 1000;

    @Getter(AccessLevel.PRIVATE)
    private final HashMap<String, SessionData> sessions;


    public ReAdaptiveOptimizerImpl() {
        this.sessions = new HashMap<>();
        runSessionMonitor();
    }

    private void runSessionMonitor() {
        // Session Monitor Thread
        Thread sessionMonitor = new Thread( new SessionMonitor(
                this.sessions,
                getInformationManager(),
                getSessionTable(),
                getInformationPages().get( "session" )
        ) );
        sessionMonitor.setUncaughtExceptionHandler( ( th, ex ) -> {
            log.error( "Uncaught exception in session monitor thread: ", ex );
        } );
        sessionMonitor.setDaemon( true );
        sessionMonitor.start();
    }

    @Override
    public String createSession() {
        final SessionData sessionData = prepareSession();
        getSessions().put( sessionData.getSessionId(), sessionData );
        return sessionData.getSessionId();
    }

    public String createSession( HashMap<String, String> parameters ) throws NumberFormatException, NullPointerException {
        int nrOfQueries = Integer.parseInt( parameters.get( "Nr. of Queries" ) );

        final QueryTemplate template = QueryUtil.createCustomTemplate( parameters );
        final String sessionId = UUID.randomUUID().toString();
        OptSession optSession = new RelQuerySession( new QuerySupplier( RelQueryGenerator.from( template ) ) );
        final SessionData sessionData = new SessionData( sessionId, template, new Thread( optSession ), optSession, nrOfQueries );

        optSession.setSessionData( sessionData );
        configureSessionThread( sessionData );
        getSessions().put( sessionData.getSessionId(), sessionData );
        return sessionData.getSessionId();
    }


    private SessionData prepareSession() {
        final String sessionId = UUID.randomUUID().toString();
        final QueryTemplate template = SessionUtil.getDefaultRelTreeTemplate();
        OptSession optSession =  new RelQuerySession( SessionUtil.getQueryGenerator( template ) );

        final SessionData sessionData = new SessionData( sessionId, template, new Thread( optSession ), optSession, DEFAULT_GEN_NR );

        optSession.setSessionData( sessionData );
        configureSessionThread( sessionData );
        return sessionData;
    }

    private void configureSessionThread( final SessionData sessionData ) {
        sessionData.getSessionThread().setName( sessionData.getSessionId() );
        sessionData.getSessionThread().setDaemon( true );
        sessionData.getSessionThread().setUncaughtExceptionHandler(
                ( Thread sessionThread, Throwable throwable ) -> {
            //initiateSessionSeedChange( sessionData, throwable )
                    log.error( "Unresolvable Session Error:", throwable );
        } );
    }

    public boolean isActive( String sid ) {
        SessionData session = getSessions().get( sid );
        return session.isStarted() && ! ( session.isFinished() || session.isMarkedForInterruption() );
    }

    public void startSession( String sid ) {
        SessionData session = getSessions().get( sid );
        session.start();
    }

    public void endSession( String sid ) {
        SessionData session = getSessions().get( sid );
        session.markForInterruption();
    }


}
