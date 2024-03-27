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

package org.polypheny.db.catalog.entity;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;


@Getter
public class MaterializedCriteria implements Serializable {


    @Setter
    private Timestamp lastUpdate;

    private final CriteriaType criteriaType;

    private final Integer interval;

    private final TimeUnit timeUnit;

    private final Long timeInMillis;

    @Setter
    private int timesUpdated;


    public MaterializedCriteria() {
        this.criteriaType = CriteriaType.MANUAL;
        this.interval = 0;
        this.timeUnit = null;
        this.lastUpdate = new Timestamp( System.currentTimeMillis() );
        this.timeInMillis = null;
        this.timesUpdated = 0;
    }


    public MaterializedCriteria( CriteriaType type ) {
        this.criteriaType = type;
        this.interval = 0;
        this.timeUnit = null;
        this.lastUpdate = new Timestamp( System.currentTimeMillis() );
        this.timeInMillis = null;
        this.timesUpdated = 0;
    }


    public MaterializedCriteria( CriteriaType type, Integer interval, TimeUnit timeUnit ) {
        this.criteriaType = type;
        this.interval = interval;
        this.timeUnit = timeUnit;
        this.lastUpdate = new Timestamp( System.currentTimeMillis() );
        this.timeInMillis = timeUnit.toMillis( interval );
        this.timesUpdated = 0;
    }


    public MaterializedCriteria( CriteriaType type, Integer interval ) {
        this.criteriaType = type;
        this.interval = interval;
        this.timeUnit = null;
        this.lastUpdate = new Timestamp( System.currentTimeMillis() );
        this.timeInMillis = null;
        this.timesUpdated = 0;
    }


    public enum CriteriaType {
        UPDATE, INTERVAL, MANUAL
    }

}
