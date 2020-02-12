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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.runtime;


import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.config.RuntimeConfig;

// NOTE:  This class gets compiled independently of everything else so that resource generation can use reflection.  That means it must have no dependencies on other Polypheny-DB code.


/**
 * Base class for all exceptions originating from Farrago.
 *
 * @see PolyphenyDbContextException
 */
@Slf4j
public class PolyphenyDbException extends RuntimeException {

    private static final long serialVersionUID = -1314522633397794178L;


    /**
     * Creates a new PolyphenyDbException object.
     *
     * @param message error message
     * @param cause underlying cause
     */
    public PolyphenyDbException( String message, Throwable cause ) {
        super( message, cause );

        // TODO: Force the caller to pass in a Logger as a trace argument for better context.  Need to extend ResGen for this.
        log.trace( "PolyphenyDbException", this );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            log.error( toString() );
        }
    }


    /**
     * Creates a new PolyphenyDbException object.
     *
     * @param message error message
     */
    public PolyphenyDbException( String message ) {
        super( message );

        // TODO: Force the caller to pass in a Logger as a trace argument for better context.  Need to extend ResGen for this.
        log.trace( "PolyphenyDbException", this );
        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            log.error( toString() );
        }
    }
}

