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

package org.polypheny.db.monitoring.statistics;


import lombok.Getter;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Contains stat data for a column
 */
public class StatisticQueryResult extends QueryResult {

    /**
     * All specified statistics for a column identified by their keys
     */
    @Getter
    private PolyValue[] data;


    /**
     * Builds a StatColumn with the individual statistics of a column
     *
     * @param data map consisting of different values to a given statistic
     */
    public StatisticQueryResult( QueryResult queryResult, final PolyValue[] data ) {
        super( queryResult.getEntity(), queryResult.getColumn() );
        this.data = data;
    }


}
