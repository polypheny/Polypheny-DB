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

package org.polypheny.db.monitoring.statistics;


import lombok.Getter;
import org.polypheny.db.type.PolyType;


/**
 * Contains stat data for a column
 */
public class StatisticQueryColumn extends QueryColumn {

    /**
     * All specified statistics for a column identified by their keys
     */
    @Getter
    private String[] data;


    /**
     * Builds a StatColumn with the individual statistics of a column
     *
     * @param type db type of the column
     * @param data map consisting of different values to a given statistic
     */
    public StatisticQueryColumn( String schemaTableName, final PolyType type, final String[] data ) {
        super( schemaTableName, type );
        this.data = data;
    }


    public StatisticQueryColumn( String schema, String table, String name, final PolyType type, final String[] data ) {
        super( schema, table, name, type );
        this.data = data;
    }

}
