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

package org.polypheny.db.monitoring.obsolet.subscriber;


import org.polypheny.db.monitoring.obsolet.MonitorEvent;


/**
 * A Subscriber registers to 1..n monitoring events.
 * The Subscriber receives callbacks whenever an event with the specific characteristics has occured.
 * Use Monitoring Subscriber as a persistence and to preprocess and aggregate items for specific and individual use cases.
 * Although each MonitorEvent is already persisted it might be useful to preaggregate certain information later on.
 */
public interface Subscriber {

    String getSubscriptionTitle();

    boolean isPersistent();

    /**
     *
     * @param event
     */
    void handleEvent( MonitorEvent event );
}
