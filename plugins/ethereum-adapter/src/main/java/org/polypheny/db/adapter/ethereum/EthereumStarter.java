/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.adapter.ethereum;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.processing.TransactionExtension;
import org.polypheny.db.transaction.TransactionManager;

// helper method, because Polypheny will create the TransactionManager (TM) relatively late
// Polypheny will startup and then get all the plugins
// But at this point there is no access to the TM
// We just say here, hey this is a TransactionExtension that says: Hey this an extension that the TM needs, please call this too as soon as we have the TM
@Slf4j
public class EthereumStarter implements TransactionExtension {

    @Override
    public void initExtension( TransactionManager manager, Authenticator authenticator ) {
        EventCacheManager.getAndSet( manager );
    }

}
