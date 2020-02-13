/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.processing;


import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.statistic.StatisticQueryProcessor;
import org.polypheny.db.transaction.TransactionManager;


@Slf4j
public class SqlQueryInterface extends QueryInterface {

    @Getter
    StatisticQueryProcessor statisticQueryProcessor;


    /**
     * Interface to enable "services" to push transactions to the transactionManager
     * TODO: decide if way to go without server
     */
    public SqlQueryInterface( TransactionManager transactionManager, Authenticator authenticator ) {
        super( transactionManager, authenticator );
    }


    @Override
    public void run() {
        this.statisticQueryProcessor = new StatisticQueryProcessor( this.transactionManager, "pa", "APP" );
    }

}
