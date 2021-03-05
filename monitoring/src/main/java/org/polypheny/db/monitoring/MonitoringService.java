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
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.HealthCheck;
import com.influxdb.client.domain.HealthCheck.StatusEnum;
import com.influxdb.client.domain.WritePrecision;
import java.time.Instant;


public class MonitoringService {
    static InfluxDBClient client;

    // InfluxDB needs to be started to use monitoring in a proper way.
    // I tested the implementation with the docker image, working just fine and explained here:
    // https://docs.influxdata.com/influxdb/v2.0/get-started/?t=Docker#

    // You can generate a Token from the "Tokens Tab" in the UI
    // TODO: Add your own token and config here!

    static String token = "EvyOwXhnCxKwAd25pUq41o3n3O3um39qi8bRtr134adzzUu_vCyxFJ8mKLqHeQ0MRpt6uEiH3dkkhL6gkctzpw==";
    static String bucket = "polypheny-monitoring";
    static String org = "unibas";
    static String url = "http://localhost:8086";

    // For influxDB testing purpose
    public static void main(final String[] args) {

        InfluxDBClient client = InfluxDBClientFactory.create(url, token.toCharArray());

        InfluxPojo data = new InfluxPojo( "sql statement", "sql statement type", 5);
        try ( WriteApi writeApi = client.getWriteApi()) {
            writeApi.writeMeasurement(bucket, org, WritePrecision.NS, data);
        }

        client.close();
    }

    public static void InitializeClient(){
        if(client == null) {
            client = InfluxDBClientFactory.create("http://localhost:8086", token.toCharArray());
        }
    }

    public static void MonitorEvent(InfluxPojo data){
        // check if client is initialized
        if( client == null){
            InitializeClient();
        }

        // check if client is available
        if (client != null) {
            HealthCheck healthCheck = client.health();
            if(healthCheck.getStatus() == StatusEnum.PASS) {
                try ( WriteApi writeApi = client.getWriteApi()) {
                    writeApi.writeMeasurement(bucket, org, WritePrecision.NS, data);
                    writeApi.flush();
                }
            }
        }
    }

    @Measurement( name = "Query" )
    public static class InfluxPojo{

        public InfluxPojo( String sql, String type, Integer numberCols ) {
            this.sql = sql;
            this.type = type;
            this.numberCols = numberCols;

            this.time = Instant.now();
        }

        @Column
        String sql;

        @Column
        String type;

        @Column
        Integer numberCols;

        @Column(timestamp = true)
        Instant time;
    }
}

