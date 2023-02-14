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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.information.Information;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationGraph;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationProgress;
import org.polypheny.db.information.InformationProgress.ProgressColor;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.information.InformationTable;

@Slf4j
public class SessionMonitor implements Runnable {
    private static final int UI_SESSION_MONITOR_UPDATE_FREQ = 1000; // ms
    private static final int SESSION_TIME_LIMIT = Integer.MAX_VALUE;

    @Getter(AccessLevel.PRIVATE)
    private final Set<String> discoveredSessions;
    @Getter(AccessLevel.PRIVATE)
    private final HashMap<String, SessionData> sessions;
    @Getter(AccessLevel.PRIVATE)
    private final InformationManager informationManager;
    @Getter(AccessLevel.PRIVATE)
    private final InformationPage informationPage;
    @Getter(AccessLevel.PRIVATE)
    private final InformationTable informationTable;

    // Demo Purposes
    private final InformationGroup group;
    //

    public SessionMonitor( final HashMap<String, SessionData> sessions,
            final InformationManager informationManager,
            final InformationTable informationTable,
            final InformationPage informationPage ) {

        this.discoveredSessions = new HashSet<>();
        this.sessions = sessions;
        this.informationManager = informationManager;
        this.informationPage = informationPage;
        this.informationTable = informationTable;

        group = new InformationGroup( "g", informationPage.getId(), "display-group" );
        informationManager.addGroup( group );
    }


    @SneakyThrows
    @Override
    public void run() {
        while ( true ) {
            Thread.sleep( UI_SESSION_MONITOR_UPDATE_FREQ );

            // Wait and continue if empty
            if ( getSessions().isEmpty() ) {
                continue;
            }

            // otherwise, inspect active sessions
            SessionData session;
            for ( String sid : getSessions().keySet() ) {
                session = getSessions().get( sid );
                if ( session.isFinished() ) {
                    // Finish Session
                    if ( log.isDebugEnabled() ) {
                        log.debug( "Session Monitor: Concluding {}", sid );
                    }
                    (( InformationProgress) getInformationManager().getInformation( sid + "-progress-bar" )).updateProgress( 100 );
                    (( InformationProgress ) getInformationManager().getInformation( sid + "-progress-bar" )).setColor( ProgressColor.GREEN );
                } else if ( session.isMarkedForInterruption() || session.getCurrentTime() > SESSION_TIME_LIMIT ) {
                    // Interrupt Session
                    if ( log.isDebugEnabled() ) {
                        log.debug( "Session Monitor: Interrupting {}", sid );
                    }
                    session.interrupt();
                    (( InformationProgress ) getInformationManager().getInformation( sid + "-progress-bar" )).setColor( ProgressColor.RED );
                } else {
                    session.xAdd( UI_SESSION_MONITOR_UPDATE_FREQ );

                    // Handle In-Session Monitoring
                    String groupName = "Session-" + sid.substring( 0, 5 );
                    InformationGroup group =  new InformationGroup( sid, getInformationPage().getId(), groupName );
                    if ( getDiscoveredSessions().contains( sid ) ) {
                        updateInformationElements( group, session, sid, false );
                    } else {
                        updateInformationElements( group, session, sid, true );
                        getDiscoveredSessions().add( sid );
                    }
                    continue;
                }

                // Conclude Session
                getInformationTable().addRow(
                        session.getSessionId(),
                        session.getInitialSeed(),
                        session.getFinalRuntime(),
                        session.getNumberOfErrors(),
                        session.getNumberOfExceptions(),
                        session.getTotalExecutions(),
                        session.getSeedSwitches(),
                        session.getFinalRuntime() / (double)session.getQueriesExecuted(),
                        session.getQueriesExecuted() / (double)session.getTotalExecutions(),
                        session.isFinished()
                );

                addRemoveButton( getInformationManager().getGroup( sid ) );

                getSessions().remove( sid );
                getDiscoveredSessions().remove( sid );

            }
        }
    }


    private void updateInformationElements( InformationGroup group, SessionData session, String sid, boolean register ) {
        if ( register ) {
            InformationProgress progress = new InformationProgress(
                    sid + "-progress-bar", sid, "Session Progress", 0 ).setMin( 0 ).setMax( 100 );

            InformationGraph infoGraph = new InformationGraph(
                    sid + "-line-graph", group.getId(), GraphType.LINE, session.getXAxis(),
                    new GraphData<>( "rAvg Successful", session.getSTm() ),
                    new GraphData<>( "rAvg Exception", session.getExTm() ),
                    new GraphData<>( "rAvg Error", session.getErTm() ),
                    new GraphData<>( "rAvg Total", session.getToTm() )
            );

            InformationGraph infoGraph2 = new InformationGraph(
                    sid + "-doughnut", group.getId(), GraphType.DOUGHNUT, new String[]{ "Successes", "Exceptions", "Errors" },
                    new GraphData<>("Current Count", new Integer[]{ session.getQueriesExecuted(), session.getNumberOfExceptions(), session.getNumberOfErrors() })
            );

            InformationAction cancelButton = new InformationAction(
                    group, sid + "-cancel", "Cancel", parameters -> cancel( sid ) );

            group.addInformation( progress, infoGraph, infoGraph2, cancelButton );

            getInformationPage().addGroup( group );
            getInformationManager().addGroup( group );
            getInformationManager().registerInformation( progress, infoGraph, infoGraph2, cancelButton );

        } else {

            int p = (int)(( session.getQueriesExecuted() / (double)session.getOrderedQueries() ) * 100);

            (( InformationProgress ) getInformationManager().getInformation( sid + "-progress-bar" )).updateProgress( p );

            (( InformationGraph ) getInformationManager().getInformation( sid + "-line-graph" )).updateGraph(
                    session.getXAxis(),
                    new GraphData<>( "rAvg Successful", session.getSTm() ),
                    new GraphData<>( "rAvg Exception", session.getExTm() ),
                    new GraphData<>( "rAvg Error", session.getErTm() ),
                    new GraphData<>( "rAvg Total", session.getToTm() )
            );

            (( InformationGraph ) getInformationManager().getInformation( sid + "-doughnut" )).updateGraph(
                    new String[]{ "Successes", "Exceptions", "Errors" },
                    new GraphData<>("Current Count", new Integer[]{ session.getQueriesExecuted(), session.getNumberOfExceptions(), session.getNumberOfErrors() })
            );
        }
    }

    private String cancel( String sid ) {
        SessionData sessionData = getSessions().get( sid );
        sessionData.interrupt();
        getInformationManager().getGroup( sid ).removeInformation(
                getInformationManager().getInformation( sid + "-cancel" )
        );
        return "Cancelled";
    }

    private void addRemoveButton( InformationGroup group ) {
        InformationAction removeButton = new InformationAction( group, group.getId() + "-remove",
                "Remove", parameters -> removeGroup( group.getId() ) );
        group.addInformation( removeButton );
        getInformationManager().registerInformation( removeButton );
    }

    private String removeGroup( String sid ) {
        getInformationManager().removeGroup(
                getInformationManager().getGroup( sid )
        );
        return "Removed";
    }

}