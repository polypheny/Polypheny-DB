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


public class MaterializedViewCriteria implements Serializable {


    @Setter
    @Getter
    private Timestamp lastUpdate;

    @Getter
    private final CriteriaType criteriaType;

    @Getter
    private final Long interval;

    @Getter
    private final TimeUnit timeUnit;

    @Getter
    private final Long timeInMillis;


    public MaterializedViewCriteria( Long interval, TimeUnit timeUnit ) {
        this.criteriaType = CriteriaType.INTERVAL;
        this.interval = interval;
        this.timeUnit = timeUnit;
        this.lastUpdate = new Timestamp( System.currentTimeMillis() );
        this.timeInMillis = timeUnit.toMillis( interval );
    }


    public enum CriteriaType {
        TIME, INTERVAL
    }


}
