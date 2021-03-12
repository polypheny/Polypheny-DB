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


import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import java.time.Instant;


@Measurement( name = "Query" )
public class InfluxPojo{
    public static InfluxPojo Create( String sql, String type, Long numberCols ){
        return new InfluxPojo( sql, type, numberCols );
    }

    public InfluxPojo(){

    }

    private InfluxPojo( String sql, String type, Long numberCols ) {
        this.sql = sql;
        this.type = type;
        this.numberCols = numberCols;

        this.time = Instant.now();
    }

    @Column
    String sql;

    @Column
    String type;

    @Column()
    Long numberCols;

    @Column(timestamp = true)
    Instant time;

    @Override
    public String toString() {
        return String.format( "%s; %s; %n; %s", sql, type, numberCols, time.toString() );
    }
}
