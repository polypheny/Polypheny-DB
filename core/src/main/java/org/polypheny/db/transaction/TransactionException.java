/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.transaction;


import javax.annotation.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.runtime.SqlStateProvider;

public class TransactionException extends GenericRuntimeException implements SqlStateProvider {

    private final String sqlState;

    public TransactionException( Exception e ) {
        super( e );
        this.sqlState = null;
    }


    public TransactionException( String message ) {
        this( message, null );
    }


    public TransactionException( String message, @Nullable String sqlState ) {
        super( message );
        this.sqlState = sqlState;
    }


    @Override
    public String getSqlState() {
        return sqlState;
    }

}
