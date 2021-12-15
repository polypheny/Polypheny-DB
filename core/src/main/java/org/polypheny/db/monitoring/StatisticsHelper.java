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

import java.util.concurrent.ConcurrentHashMap;
import lombok.Getter;

public class StatisticsHelper {

    private static volatile StatisticsHelper instance = null;


    public static StatisticsHelper getInstance() {

        if ( instance == null ) {
            instance = new StatisticsHelper();
        }

        return instance;
    }


    @Getter
    public ConcurrentHashMap<Long, Integer> tableRowCount = new java.util.concurrent.ConcurrentHashMap<>();

}