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

package org.polypheny.db.monitoring;


import com.influxdb.LogLevel;
import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.HealthCheck;
import com.influxdb.client.domain.HealthCheck.StatusEnum;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.query.FluxTable;
import com.influxdb.query.internal.FluxResultMapper;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;


//ToDo add some kind of configuration which can for one decide on which backend to select, if we might have severall like
// * InfluxDB
// * File
// * map db
// * etc
@Slf4j
public class MonitoringService {

    private final String MONITORING_BACKEND = "simple"; //InfluxDB
    private BackendConnector backendConnector;


    public MonitoringService(){
        initializeClient();
    }

    /**
     * This method faces should be used to add new items to backend
     * it should be invoked in directly
     *
     * It is backend agnostic and makes sure to parse and extract all necessary information
     * which should be added to the backend
     *
     * @param event to add to the queue which will registered as a new monitoring metric
     */
    public void addWorkloadEventToQueue(MonitorEvent event){


        System.out.println("\nHENNLO: Added new Worklaod event:"
                + "\n\t STMT_TYPE:" + event.monitoringType + " "
                + "\n\t Description: " + event.getDescription() + " "
                + "\n\t Timestamp " + event.getRecordedTimestamp() + " "
                + "\n\t Field Names " + event.getFieldNames());


    }


    /**
     * This is currently a dummy Service mimicking the final retrieval of monitoring data
     *
     * @param type  Search for specific workload type
     * @param filter on select worklaod type
     *
     * @return some event or statistic which can be immidiately used
     */
    public String getWorkloadItem(String type, String filter){
        System.out.println("HENNLO: Looking for: '" + type +"' with filter: '" + filter + "'");

        backendConnector.readStatisticEvent( " " );

        return "EMPTY WORKLOAD EVENT";
    }

    private void initializeClient(){
        // Get Backend currently set in monitoring
        backendConnector = BackendConnectorFactory.getBackendInstance(MONITORING_BACKEND);
    }

    private static class BackendConnectorFactory {

        //Returns backend based on configured statistic Backend in runtimeconfig
        public static BackendConnector getBackendInstance( String statisticBackend ) {
            switch ( statisticBackend ) {
                case "InfluxDB":
                    //TODO add error handling or fallback to default backend when no Influx is available
                    return new InfluxBackendConnector();

                case "simple":
                    return new SimpleBackendConnector();

                default :
                    throw new RuntimeException( "Unknown Backend type: '" + statisticBackend  + "' ");
            }


        }

    }

}

