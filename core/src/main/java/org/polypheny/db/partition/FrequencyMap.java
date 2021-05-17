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

package org.polypheny.db.partition;





/**
 * Periodically retrieves information from the MonitoringService to get current statistics about
 * the frequency map to determine which chunk of data should reside in HOT & which in COLD partition
 *
 * Only one instance of the MAP exists.
 * Which gets created once the first TEMPERATURE partitioned table gets created. (Including creation of BackgroundTask)
 * and consequently will be shutdown when no TEMPERATURE partitioned tables exist anymore
 */
public class FrequencyMap {




    public void getTableFrequency(){
        
    }

    public void getPartitionFrequency(){

    }

    public void getPartitionFrequencyOnStore(){

    }

}
