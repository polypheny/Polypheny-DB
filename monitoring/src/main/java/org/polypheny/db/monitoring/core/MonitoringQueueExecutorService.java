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

package org.polypheny.db.monitoring.core;


public class MonitoringQueueExecutorService {

    private final int CORE_POOL_SIZE = 1;
    private final int MAXIMUM_POOL_SIZE = 8;
    private final int KEEP_ALIVE_TIME_SECONDS = 10;
    //public BlockingQueue<Runnable> workQueue = new ConcurrentLinkedQueue<>();

    /*public MonitoringQueueExecutorService(){
        ThreadPoolExecutor tp = new ThreadPoolExecutor( CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, KEEP_ALIVE_TIME_SECONDS, TimeUnit.SECONDS,  );
    }*/

}
