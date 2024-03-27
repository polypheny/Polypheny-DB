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

package org.polypheny.db.monitoring.core;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.NonNull;
import org.polypheny.db.monitoring.InMemoryRepository;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;
import org.polypheny.db.util.PolyphenyHomeDirManager;


public class TestInMemoryRepository extends InMemoryRepository {

    private static final String FILE_PATH = "testDb";
    private static final String FOLDER_NAME = "monitoring";

    AtomicInteger count = new AtomicInteger();


    @Override
    public void initialize( boolean resetRepository ) {
        this.reset();
        super.initialize( FILE_PATH, FOLDER_NAME, resetRepository );
    }


    private void reset() {
        File folder = PolyphenyHomeDirManager.getInstance().registerNewFolder( FOLDER_NAME );
        new File( folder, FILE_PATH ).delete();
    }


    @Override
    public void dataPoint( @NonNull MonitoringDataPoint dataPoint ) {
        super.dataPoint( dataPoint );
        count.incrementAndGet();
    }

}
