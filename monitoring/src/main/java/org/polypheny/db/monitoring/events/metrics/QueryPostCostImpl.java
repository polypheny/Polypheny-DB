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

package org.polypheny.db.monitoring.events.metrics;

import java.io.Serializable;
import lombok.Getter;
import org.polypheny.db.monitoring.events.QueryPostCost;


public class QueryPostCostImpl implements QueryPostCost, Serializable {

    @Getter
    private final String physicalQueryClass;

    @Getter
    private final long executionTime;

    @Getter
    private final int numberOfSamples;


    public QueryPostCostImpl( String physicalQueryClass, long executionTime, int numberOfSamples ) {
        this.physicalQueryClass = physicalQueryClass;
        this.executionTime = executionTime;
        this.numberOfSamples = numberOfSamples;
    }

}
