/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.demo;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.java.AdapterTemplate;
import org.polypheny.db.catalog.entity.LogicalAdapter.AdapterType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.docker.DockerInstance;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

@Slf4j
public abstract class DemoStore {
    protected final static String ORIGIN = "Demo";
    protected final DataModel dataModel;
    protected final String name;
    protected final String adapterName;

    protected long namespaceId;

    protected Optional<DataStore<?>> dataStore = Optional.empty();

    protected DemoStore(String name, DataModel dataModel, String adapterName ) {
        this.name = name;
        this.dataModel = dataModel;
        this.adapterName = adapterName;
    }


    public void createNamespace( Statement statement ) {
        log.debug( "Creating {} namespace", this.name );
        this.namespaceId = DdlManager.getInstance().createNamespace( this.name, this.dataModel, true, true, false,  statement);
        this.dataStore = AdapterManager.getInstance().getStore( this.name );
    }

    public void createAdapter( DockerInstance dockerInstance ) {
        log.debug( "Creating {} store", this.name );
        AdapterTemplate adapterTemplate = AdapterTemplate.fromString( this.adapterName, AdapterType.STORE );
        Map<String, String> defaultSettings = adapterTemplate.getDefaultSettings();
        defaultSettings.put( "instanceId", String.valueOf(dockerInstance.getInfo().id()) );
        DdlManager.getInstance().createStore( this.name, adapterTemplate.getAdapterName(), AdapterType.STORE, defaultSettings, DeployMode.DOCKER);

    }

    public abstract void setupNamespace( Statement statement );

    public abstract void loadData();

    public Stream<String> getJarFileAsStream(String path) {
        return this.getFileAsStream( path, true );
    }

    public Stream<String> getLocalFileAsStream(String path) {
        return this.getFileAsStream( path, false );
    }


    public Stream<String> getFileAsStream(String path, boolean classpath) {
        if (path.isEmpty()) {
            log.error( "No path specified" );
            return Stream.empty();
        }

        log.debug( "Loading data from resource path {}", path );

        try {
            InputStream inputStream;
            if ( classpath ) {
                inputStream = DemoPlugin.class.getResourceAsStream( path );
            } else {
                inputStream = new FileInputStream( path );
            }

            if ( inputStream == null ) {
                log.error( "Unable to get resource stream for file {}", path );
                return Stream.empty();
            }

            InputStreamReader inputStreamReader = new InputStreamReader( inputStream, StandardCharsets.UTF_8 );
            BufferedReader bufferedReader = new BufferedReader( inputStreamReader );
            return bufferedReader.lines();
        }
        catch ( FileNotFoundException E ) {
            log.error("Unable find file {}", path);
        }

        return Stream.empty();
    }
}
