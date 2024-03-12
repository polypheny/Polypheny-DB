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

package org.polypheny.db.util;


import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.Context;


/**
 * CancelFlag is used to post and check cancellation requests.
 *
 * Pass it to {@link AlgPlanner} by putting it into a {@link Context}.
 */
public class CancelFlag {

    /**
     * The flag that holds the cancel state.
     * Feel free to use the flag directly.
     */
    public final AtomicBoolean atomicBoolean;


    public CancelFlag( AtomicBoolean atomicBoolean ) {
        this.atomicBoolean = Objects.requireNonNull( atomicBoolean );
    }


    /**
     * @return whether a cancellation has been requested
     */
    public boolean isCancelRequested() {
        return atomicBoolean.get();
    }


    /**
     * Requests a cancellation.
     */
    public void requestCancel() {
        atomicBoolean.compareAndSet( false, true );
    }


    /**
     * Clears any pending cancellation request.
     */
    public void clearCancel() {
        atomicBoolean.compareAndSet( true, false );
    }

}

