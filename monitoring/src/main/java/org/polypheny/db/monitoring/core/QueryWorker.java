/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.monitoring.core;

import com.google.gson.Gson;
import java.sql.Timestamp;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.information.InformationDuration;
import org.polypheny.db.monitoring.dtos.MonitoringJob;
import org.polypheny.db.monitoring.dtos.QueryData;
import org.polypheny.db.monitoring.dtos.QueryPersistentData;
import org.polypheny.db.monitoring.persistence.MonitoringRepository;
import org.polypheny.db.rel.RelNode;

@Slf4j
public class QueryWorker implements MonitoringQueueWorker<QueryData, QueryPersistentData> {

    private final MonitoringRepository repository;


    public QueryWorker( MonitoringRepository repository ) {
        if ( repository == null ) {
            throw new IllegalArgumentException( "repository is null" );
        }

        this.repository = repository;
    }


    @Override
    public MonitoringJob<QueryData, QueryPersistentData> handleJob( MonitoringJob<QueryData, QueryPersistentData> job ) {
        QueryData queryData = job.getMonitoringData();
        QueryPersistentData dbEntity = QueryPersistentData
                .builder()
                .description( queryData.getDescription() )
                .monitoringType( queryData.getMonitoringType() )
                .Id( job.getId() )
                .fieldNames( queryData.getFieldNames() )
                .executionTime( queryData.getExecutionTime() )
                .rowCount( queryData.getRowCount() )
                .isSubQuery( queryData.isSubQuery() )
                .recordedTimestamp( new Timestamp( queryData.getRecordedTimestamp() ) )
                .build();

        job.setMonitoringPersistentData( dbEntity );
        RelNode node = queryData.getRouted().rel;
        job = processRelNode( node, job );

        // TODO: read even more data
        // job.getMonitoringPersistentData().getDataElements()
        if ( job.getMonitoringData().isAnalyze() ) {
            try {
                InformationDuration duration = new Gson().fromJson( job.getMonitoringData().getDurations(), InformationDuration.class );
                this.getDurationInfo( job, "Index Update", duration );
                this.getDurationInfo( job, "Plan Caching", duration );
                this.getDurationInfo( job, "Index Lookup Rewrite", duration );
                this.getDurationInfo( job, "Constraint Enforcement", duration );
                this.getDurationInfo( job, "Implementation Caching", duration );
                this.getDurationInfo( job, "Routing", duration );
                this.getDurationInfo( job, "Planning & Optimization", duration );
                this.getDurationInfo( job, "Implementation", duration );
                this.getDurationInfo( job, "Locking", duration );
            } catch ( Exception e ) {
                log.debug( "could not deserialize of get duration info" );
            }

        }

        this.repository.persistJob( job );
        return job;
    }


    private void getDurationInfo( MonitoringJob<QueryData, QueryPersistentData> job, String durationName, InformationDuration duration ) {
        try {
            long time = duration.getDuration( durationName );
            job.getMonitoringPersistentData().getDataElements().put( durationName, time );
        } catch ( Exception e ) {
            log.debug( "could no find duration:" + durationName );
        }
    }


    //@Cedric should every Worker really to this?
    // One time is sufficient to do this.
    // For me workers are a central part of the monitoring system and should therefore genarealize as much as possible. Whereas subscribers e.g.
    //should be used to make more specific stuff.
    //TODO Discuss
    private MonitoringJob<QueryData, QueryPersistentData> processRelNode( RelNode node, MonitoringJob<QueryData, QueryPersistentData> currentJob ) {

        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            processRelNode( node.getInput( i ), currentJob );
        }


        if ( node.getTable() != null ) {
            currentJob.getMonitoringPersistentData().getTables().addAll( node.getTable().getQualifiedName() );
        }

        return currentJob;
    }

}
