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

package org.polypheny.db.routing;

import java.util.HashMap;
import java.util.Map;

public class ExecutionTimeMonitor {

    private final Map<ExecutionTimeObserver, String> observers = new HashMap<>(); // Observer and their individual reference


    public void setExecutionTime( long nanoTime ) {
        for ( Map.Entry<ExecutionTimeObserver, String> observerEntry : observers.entrySet() ) {
            observerEntry.getKey().executionTime( observerEntry.getValue(), nanoTime );
        }
    }


    public void subscribe( ExecutionTimeObserver observer, String queryClassString ) {
        observers.put( observer, queryClassString );
    }


    public interface ExecutionTimeObserver {

        void executionTime( String reference, long nanoTime );

    }

}
