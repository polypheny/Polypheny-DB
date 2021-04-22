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


import lombok.Setter;
import org.polypheny.db.monitoring.obsolet.MonitorEvent;
import org.polypheny.db.monitoring.obsolet.storage.BackendConnector;


public abstract class AbstractSubscriber implements Subscriber{

    @Setter
    protected String subscriberName;
    protected BackendConnector backendConnector;


    protected boolean isPersistent;

    public String getSubscriptionTitle(){
        return subscriberName;
    }

    protected BackendConnector initializePersistence(){
    //If the subscriber wants to have a persistency for his entries
    // this method will be invoked to retrieve and setup the system defined BackendConnector
        return null;
    }


    protected abstract void initializeSubscriber();

    protected abstract void initPersistentDB();

    @Override
    public boolean isPersistent() {
        return isPersistent;
    }


    @Override
    public abstract void handleEvent( MonitorEvent event );
}
