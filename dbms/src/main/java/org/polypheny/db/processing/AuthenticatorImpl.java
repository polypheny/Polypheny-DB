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


import java.util.Optional;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.iface.AuthenticationException;
import org.polypheny.db.iface.Authenticator;


/**
 *
 */
public class AuthenticatorImpl implements Authenticator {

    @Override
    public CatalogUser authenticate( final String username, final String password ) throws AuthenticationException {
        Optional<CatalogUser> catalogUser = Catalog.getInstance().getSnapshot().getUser( username );
        if ( catalogUser.isEmpty() ) {
            throw new AuthenticationException( "There exists no user with the username " + username );
        }

        /******************************************************************
         *                                                                *
         *  WARNING!                                                      *
         *                                                                *
         *  Until user authentication is fully implemented,               *
         *  this bypass ensures backwards compatibility                   *
         *  with the old default password which was an empty string.      *
         *                                                                *
         *  This should be removed upon implementation                    *
         *  of user authentication!                                       *
         *                                                                *
         *  TODO: remove this upon implementation of user authentication  *
         *                                                                *
         ******************************************************************/
        if ( password == null || password.isEmpty() ) {
            return catalogUser.get();
        }
        /******************************************************************/

        if ( catalogUser.get().password.equals( password ) ) {
            return catalogUser.get();
        } else {
            throw new AuthenticationException("Wrong password for user '" + username + "'!");
        }
    }

}
