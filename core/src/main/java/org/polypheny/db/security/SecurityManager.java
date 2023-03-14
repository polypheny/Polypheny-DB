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

package org.polypheny.db.security;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;


public class SecurityManager {

    private static SecurityManager INSTANCE = null;

    private final Map<Path, AuthStatus> status = new ConcurrentHashMap<>();


    private SecurityManager() {
        // empty on purpose
    }


    public static SecurityManager getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new SecurityManager();
        }
        return INSTANCE;
    }


    @Nullable // if path has access already
    public UUID requestPathAccess( String uniqueName, String requester, Path path ) {
        Path dir = path;
        if ( dir.toFile().isFile() ) {
            dir = dir.getParent();
        }
        if ( status.containsKey( dir ) ) {
            // send old or done uuid
            if ( status.get( dir ).step == AuthStep.SUCCESSFUL ) {
                return null;
            }
            return status.get( dir ).uuid;
        }

        UUID uuid = UUID.randomUUID();
        status.put( dir, new AuthStatus( uniqueName, uuid, dir, requester ) );

        return uuid;
    }


    public boolean checkPathAccess( Path path ) {
        Path dir = path;
        if ( dir.toFile().isFile() ) {
            dir = dir.getParent();
        }
        AuthStatus status = this.status.get( dir );

        if ( status.step == AuthStep.SUCCESSFUL ) {
            return true;
        }
        if ( Arrays.stream( Objects.requireNonNull( status.path.toFile().listFiles() ) ).noneMatch( f -> f.getName().equals( "polypheny.access" ) && f.isFile() ) ) {
            // TODO: if more fine-grained access control is required, add as content of file
            return false;
        }

        status.setStep( AuthStep.SUCCESSFUL );
        return true;
    }


    private enum AuthStep {
        INITIAL,
        SUCCESSFUL
    }


    @Getter
    private static class AuthStatus {

        private final String name;
        private final Path path;
        private final String requester;
        private final UUID uuid;
        @Setter
        private AuthStep step = AuthStep.INITIAL;


        private AuthStatus( String name, UUID uuid, Path path, String requester ) {
            this.name = name;
            this.path = path;
            this.uuid = uuid;
            this.requester = requester;
        }

    }

}
