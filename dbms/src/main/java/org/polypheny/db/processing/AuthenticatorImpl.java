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

package org.polypheny.db.processing;


import java.nio.charset.StandardCharsets;
import org.bouncycastle.util.Arrays;
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
        LogicalUser logicalUser = Catalog.getInstance().getSnapshot().getUser( username ).orElseThrow( () -> new AuthenticationException( "Wrong username or password" ) );
        if ( Arrays.constantTimeAreEqual( logicalUser.password.getBytes( StandardCharsets.UTF_8 ), password.getBytes( StandardCharsets.UTF_8 ) ) ) {
            return logicalUser;
        } else {
            throw new AuthenticationException( "Wrong username or password" );
        }
    }

}
