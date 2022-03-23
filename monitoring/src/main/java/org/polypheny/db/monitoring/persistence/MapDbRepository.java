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

package org.polypheny.db.monitoring.persistence;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBException;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.polypheny.db.StatusService;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.monitoring.events.MonitoringDataPoint;
import org.polypheny.db.monitoring.events.QueryPostCost;
import org.polypheny.db.monitoring.events.metrics.QueryPostCostImpl;
import org.polypheny.db.monitoring.repository.PersistentMonitoringRepository;
import org.polypheny.db.util.PolyphenyHomeDirManager;

@Slf4j
public class MapDbRepository implements PersistentMonitoringRepository {

    private static final String FILE_PATH = "simpleBackendDb";
    private static final String FOLDER_NAME = "monitoring";
    protected final HashMap<Class, BTreeMap<UUID, MonitoringDataPoint>> data = new HashMap<>();
    protected DB simpleBackendDb;
    protected BTreeMap<String, QueryPostCostImpl> queryPostCosts;


    @Override
    public void initialize( boolean resetRepository ) {
        this.initialize( FILE_PATH, FOLDER_NAME, resetRepository );
    }


    @Override
    public void dataPoint( @NonNull MonitoringDataPoint dataPoint ) {
        BTreeMap table = this.data.get( dataPoint.getClass() );
        if ( table == null ) {
            this.createPersistentTable( dataPoint.getClass() );
            table = this.data.get( dataPoint.getClass() );
        }

        if ( table != null && dataPoint != null ) {
            table.put( dataPoint.id(), dataPoint );
            this.simpleBackendDb.commit();
        }
    }


    @Override
    public <TPersistent extends MonitoringDataPoint> List<TPersistent> getAllDataPoints( @NonNull Class<TPersistent> dataPointClass ) {
        final Map<UUID, MonitoringDataPoint> table = this.data.get( dataPointClass );
        if ( table != null ) {
            return table.values()
                    .stream()
                    .map( monitoringPersistentData -> (TPersistent) monitoringPersistentData )
                    .sorted( Comparator.comparing( MonitoringDataPoint::timestamp ).reversed() )
                    .collect( Collectors.toList() );
        }

        return Collections.emptyList();
    }


    @Override
    public <TPersistent extends MonitoringDataPoint> long getNumberOfDataPoints( @NonNull Class<TPersistent> dataPointClass ) {
        final Map<UUID, MonitoringDataPoint> table = this.data.get( dataPointClass );
        if ( table != null ) {
            return table.values().size();
        }
        return 0;
    }


    @Override
    public <T extends MonitoringDataPoint> List<T> getDataPointsBefore( @NonNull Class<T> dataPointClass, @NonNull Timestamp timestamp ) {
        final Map<UUID, MonitoringDataPoint> table = this.data.get( dataPointClass );
        if ( table != null ) {
            return table.values()
                    .stream()
                    .map( monitoringPersistentData -> (T) monitoringPersistentData )
                    .sorted( Comparator.comparing( MonitoringDataPoint::timestamp ).reversed() )
                    .filter( elem -> elem.timestamp().before( timestamp ) )
                    .collect( Collectors.toList() );
        }

        return Collections.emptyList();
    }


    @Override
    public <T extends MonitoringDataPoint> List<T> getDataPointsAfter( @NonNull Class<T> dataPointClass, @NonNull Timestamp timestamp ) {
        final Map<UUID, MonitoringDataPoint> table = this.data.get( dataPointClass );
        if ( table != null ) {
            return table.values()
                    .stream()
                    .map( monitoringPersistentData -> (T) monitoringPersistentData )
                    .sorted( Comparator.comparing( MonitoringDataPoint::timestamp ).reversed() )
                    .filter( elem -> elem.timestamp().after( timestamp ) )
                    .collect( Collectors.toList() );
        }

        return Collections.emptyList();
    }


    /**
     * Removes all data points for given monitoring persistent type.
     *
     * @param dataPointClass specific datapoint class of interest to remove
     */
    @Override
    public <T extends MonitoringDataPoint> void removeAllDataPointsOfSpecificClass( Class<T> dataPointClass ) {
        if ( data.containsKey( dataPointClass ) ) {
            data.remove( dataPointClass );
        }
    }


    /**
     * Removes all aggregated dataPoints.
     */
    @Override
    public void resetAllDataPoints() {
        if ( data.isEmpty() ) {
            return;
        }
        data.clear();
        this.simpleBackendDb.commit();
    }


    private List<Class> getAllDataPointClasses() {
        return data.keySet().stream().collect( Collectors.toList() );
    }


    @Override
    public QueryPostCost getQueryPostCosts( @NonNull String physicalQueryClass ) {
        if ( queryPostCosts == null ) {
            this.initializePostCosts();
        }

        QueryPostCost result = queryPostCosts.get( physicalQueryClass );
        return result != null ? result : new QueryPostCostImpl( physicalQueryClass, 0, 0 );
    }


    @Override
    public List<QueryPostCost> getAllQueryPostCosts() {
        if ( queryPostCosts == null ) {
            this.initializePostCosts();
        }

        return new ArrayList<>( queryPostCosts.values() );
    }


    @Override
    public void updateQueryPostCosts( @NonNull String physicalQueryClass, long executionTime ) {
        if ( queryPostCosts == null ) {
            this.initializePostCosts();
            return;
        }

        final QueryPostCostImpl result = queryPostCosts.get( physicalQueryClass );
        if ( result == null ) {
            queryPostCosts.put( physicalQueryClass, new QueryPostCostImpl( physicalQueryClass, executionTime, 1 ) );

        } else {
            long newTotalTime = (result.getExecutionTime() * result.getNumberOfSamples()) + executionTime;
            int samples = result.getNumberOfSamples() + 1;
            long newTime = newTotalTime / samples;
            queryPostCosts.replace( physicalQueryClass, new QueryPostCostImpl( physicalQueryClass, newTime, samples ) );
        }

        this.simpleBackendDb.commit();
    }


    @Override
    public void resetQueryPostCosts() {
        if ( queryPostCosts == null ) {
            return;
        }
        queryPostCosts.clear();
        this.simpleBackendDb.commit();
    }


    protected void initialize( String filePath, String folderName, boolean resetRepository ) {
        if ( simpleBackendDb != null ) {
            simpleBackendDb.close();
        }

        synchronized ( this ) {
            File folder = PolyphenyHomeDirManager.getInstance().registerNewFolder( folderName );

            if ( Catalog.resetCatalog ) {
                StatusService.printInfo( "Resetting monitoring repository on startup." );

                if ( new File( folder, filePath ).exists() ) {
                    new File( folder, filePath ).delete();
                }
            }

            // Assume that file is locked
            boolean fileLocked = true;
            long secondsToWait = 30;

            long timeThreshold = secondsToWait * 1000;

            long start = System.currentTimeMillis();
            long finish = System.currentTimeMillis();
            while ( fileLocked && ((finish - start) < timeThreshold) ) {
                try {
                    simpleBackendDb = DBMaker
                            .fileDB( new File( folder, filePath ) )
                            .closeOnJvmShutdown()
                            .transactionEnable()
                            .fileMmapEnableIfSupported()
                            .fileMmapPreclearDisable()
                            .make();
                    fileLocked = false;
                } catch ( DBException e ) {
                    log.warn( "Monitoring Repository is currently locked by another process. Waiting..." );
                }
                finish = System.currentTimeMillis();
            }
            // Exceeded threshold
            if ( (finish - start) >= timeThreshold ) {
                throw new RuntimeException( "Initializing Monitoring Repository took too long...\nMake sure that no other "
                        + "instance of Polypheny-DB has still locked the monitoring information.\n"
                        + "Wait a few seconds or stop the locking process and try again. " );
            }

            simpleBackendDb.getStore().fileLoad();
        }
    }


    private void initializePostCosts() {
        queryPostCosts = simpleBackendDb.treeMap( QueryPostCost.class.getName(), Serializer.STRING, Serializer.JAVA ).createOrOpen();
    }


    private void createPersistentTable( Class<? extends MonitoringDataPoint> classPersistentData ) {
        if ( classPersistentData != null ) {
            final BTreeMap<UUID, MonitoringDataPoint> treeMap = simpleBackendDb.treeMap( classPersistentData.getName(), Serializer.UUID, Serializer.JAVA ).createOrOpen();
            data.put( classPersistentData, treeMap );
        }
    }

}
