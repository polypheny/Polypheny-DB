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

package org.polypheny.db.docker.exceptions;

import lombok.Getter;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;

/**
 * These are exceptions with contents that are suitable to display a user
 */
public class DockerUserException extends GenericRuntimeException {

    @Getter
    int status = 400;


    public DockerUserException( String message, Object... params ) {
        super( message, params );
    }


    public DockerUserException( String message, Throwable e, Object... params ) {
        super( message, e, params );
    }


    public DockerUserException( Throwable e ) {
        super( e );
    }


    public DockerUserException( int status, String message, Object... params ) {
        super( message, params );
        this.status = status;
    }

}
