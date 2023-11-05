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

package org.polypheny.db.processing;


import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalUser;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.iface.Authenticator;


/**
 *
 */
public class AuthenticatorImpl implements Authenticator {

    @Override
    public LogicalUser authenticate( final String username, final String password ) throws AuthenticationException {
        LogicalUser logicalUser = Catalog.getInstance().getSnapshot().getUser( username ).orElseThrow();
        if ( logicalUser.password.equals( password ) ) {
            return logicalUser;
        } else {
            throw new AuthenticationException( "Wrong password for user '" + username + "'!" );
        }
    }

}
