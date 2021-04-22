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

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.polypheny.db.monitoring.dtos.MonitoringJob;
import org.polypheny.db.util.FileSystemManager;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class MapDbRepository implements WriteMonitoringRepository, ReadOnlyMonitoringRepository {

    private static final String FILE_PATH = "simpleBackendDb-cm";
    private static final String FOLDER_NAME = "monitoring";
    private DB simpleBackendDb;

    // private final HashMap<Class, BTreeMap<UUID, MonitoringEventData>> tables = new HashMap<>();
    private final HashMap<Class, BTreeMap<UUID, MonitoringPersistentData>> data = new HashMap<>();

    @Override
    public void initialize() {

        if (simpleBackendDb != null) {
            simpleBackendDb.close();
        }

        File folder = FileSystemManager.getInstance().registerNewFolder(this.FOLDER_NAME);

        simpleBackendDb = DBMaker.fileDB(new File(folder, this.FILE_PATH))
                .closeOnJvmShutdown()
                .transactionEnable()
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .make();

        simpleBackendDb.getStore().fileLoad();
    }


    private void createPersistentTable(Class<? extends MonitoringPersistentData> classPersistentData) {
        if (classPersistentData != null) {
            val treeMap = simpleBackendDb.treeMap(classPersistentData.getName(), Serializer.UUID, Serializer.JAVA).createOrOpen();
            data.put(classPersistentData, treeMap);
        }
    }

    @Override
    public void writeEvent(MonitoringJob job) {
        val table = this.data.get(job.getPersistentData().getClass());
        if (table == null) {
            this.createPersistentTable(job.getPersistentData().getClass());
            this.writeEvent(job);
        }

        if (table != null && job.getPersistentData() != null) {
            table.put(job.Id(), job.getPersistentData());
            this.simpleBackendDb.commit();
        }
    }

    @Override
    public <TPersistent extends MonitoringPersistentData> List<TPersistent> GetAll(Class<TPersistent> classPersistent) {
        val table = this.data.get(classPersistent);
        if (table != null) {
            return table.entrySet()
                    .stream()
                    .map(elem -> (TPersistent) elem.getValue())
                    .sorted(Comparator.comparing(MonitoringPersistentData::timestamp).reversed())
                    .collect(Collectors.toList());
        }

        return Collections.emptyList();
    }

}
