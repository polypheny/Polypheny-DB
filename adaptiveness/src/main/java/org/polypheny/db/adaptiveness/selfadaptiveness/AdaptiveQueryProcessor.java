/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adaptiveness.selfadaptiveness;

import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.Transaction.MultimediaFlavor;
import org.polypheny.db.transaction.TransactionManager;

public class AdaptiveQueryProcessor {

    private final TransactionManager transactionManager;
    private final String databaseName;
    private final String userName;


    public AdaptiveQueryProcessor( final TransactionManager transactionManager, String userName, String databaseName ) {
        this.transactionManager = transactionManager;
        this.databaseName = databaseName;
        this.userName = userName;
    }


    public AdaptiveQueryProcessor( TransactionManager transactionManager, Authenticator authenticator ) {
        this( transactionManager, "pa", "APP" );
    }


    public Transaction getTransaction() {
        try {
            return transactionManager.startTransaction( userName, databaseName, false, "SELF-ADAPTIVE-SYSTEM", MultimediaFlavor.FILE );
        } catch ( UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
    }

}
