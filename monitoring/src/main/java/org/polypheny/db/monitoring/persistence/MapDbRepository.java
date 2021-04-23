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

package org.polypheny.db.monitoring.persistence;

import java.io.File;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.polypheny.db.monitoring.dtos.MonitoringData;
import org.polypheny.db.monitoring.dtos.MonitoringJob;
import org.polypheny.db.monitoring.dtos.MonitoringPersistentData;
import org.polypheny.db.monitoring.persistent.MonitoringRepository;
import org.polypheny.db.monitoring.persistent.ReadOnlyMonitoringRepository;
import org.polypheny.db.util.FileSystemManager;

@Slf4j
public class MapDbRepository implements MonitoringRepository, ReadOnlyMonitoringRepository {

    // region private fields

    private static final String FILE_PATH = "simpleBackendDb-cm";
    private static final String FOLDER_NAME = "monitoring";
    private final HashMap<Class, BTreeMap<UUID, MonitoringPersistentData>> data = new HashMap<>();
    private DB simpleBackendDb;

    // endregion

    // region public methods


    @Override
    public void initialize() {

        if ( simpleBackendDb != null ) {
            simpleBackendDb.close();
        }

        File folder = FileSystemManager.getInstance().registerNewFolder( FOLDER_NAME );

        simpleBackendDb = DBMaker.fileDB( new File( folder, FILE_PATH ) )
                .closeOnJvmShutdown()
                .transactionEnable()
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .make();

        simpleBackendDb.getStore().fileLoad();
    }


    @Override
    public <TEvent extends MonitoringData, TPersistent extends MonitoringPersistentData> void persistJob( MonitoringJob<TEvent, TPersistent> job ) {
        if ( job == null || job.getMonitoringPersistentData() == null ) {
            throw new IllegalArgumentException( "invalid argument null" );
        }

        val table = this.data.get( job.getMonitoringPersistentData().getClass() );
        if ( table == null ) {
            this.createPersistentTable( job.getMonitoringPersistentData().getClass() );
            this.persistJob( job );
        }

        if ( table != null && job.getMonitoringPersistentData() != null ) {
            table.put( job.getId(), job.getMonitoringPersistentData() );
            this.simpleBackendDb.commit();
        }
    }


    @Override
    public <TPersistent extends MonitoringPersistentData> List<TPersistent> GetAll( Class<TPersistent> classPersistent ) {
        val table = this.data.get( classPersistent );
        if ( table != null ) {
            return table.values()
                    .stream()
                    .map( monitoringPersistentData -> (TPersistent) monitoringPersistentData )
                    .sorted( Comparator.comparing( MonitoringPersistentData::timestamp ).reversed() )
                    .collect( Collectors.toList() );
        }

        return Collections.emptyList();
    }


    @Override
    public <TPersistent extends MonitoringPersistentData> List<TPersistent> GetBefore( Class<TPersistent> classPersistent, Timestamp timestamp ) {
        // TODO: not tested yet
        val table = this.data.get( classPersistent );
        if ( table != null ) {
            return table.values()
                    .stream()
                    .map( monitoringPersistentData -> (TPersistent) monitoringPersistentData )
                    .sorted( Comparator.comparing( MonitoringPersistentData::timestamp ).reversed() )
                    .filter( elem -> elem.timestamp().before( timestamp ) )
                    .collect( Collectors.toList() );
        }

        return Collections.emptyList();
    }


    @Override
    public <TPersistent extends MonitoringPersistentData> List<TPersistent> GetAfter( Class<TPersistent> classPersistent, Timestamp timestamp ) {
        // TODO: not tested yet
        val table = this.data.get( classPersistent );
        if ( table != null ) {
            return table.values()
                    .stream()
                    .map( monitoringPersistentData -> (TPersistent) monitoringPersistentData )
                    .sorted( Comparator.comparing( MonitoringPersistentData::timestamp ).reversed() )
                    .filter( elem -> elem.timestamp().after( timestamp ) )
                    .collect( Collectors.toList() );
        }

        return Collections.emptyList();
    }

    // endregion

    // region private helper methods


    private void createPersistentTable( Class<? extends MonitoringPersistentData> classPersistentData ) {
        if ( classPersistentData != null ) {
            val treeMap = simpleBackendDb.treeMap( classPersistentData.getName(), Serializer.UUID, Serializer.JAVA ).createOrOpen();
            data.put( classPersistentData, treeMap );
        }
    }

    // endregion

}
