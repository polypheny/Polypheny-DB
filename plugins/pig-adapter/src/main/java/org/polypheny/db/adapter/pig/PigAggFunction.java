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

package org.polypheny.db.adapter.pig;


import org.polypheny.db.algebra.constant.Kind;


/**
 * Supported Pig aggregate functions and their Polypheny-DB counterparts. The enum's name() is the same as the function's name in Pig Latin.
 */
public enum PigAggFunction {

    COUNT( Kind.COUNT, false ), COUNT_STAR( Kind.COUNT, true );

    private final Kind polyphenyDbFunc;
    private final boolean star; // as in COUNT(*)


    PigAggFunction( Kind polyphenyDbFunc ) {
        this( polyphenyDbFunc, false );
    }


    PigAggFunction( Kind polyphenyDbFunc, boolean star ) {
        this.polyphenyDbFunc = polyphenyDbFunc;
        this.star = star;
    }


    public static PigAggFunction valueOf( Kind polyphenyDb, boolean star ) {
        for ( PigAggFunction pigAggFunction : values() ) {
            if ( pigAggFunction.polyphenyDbFunc == polyphenyDb && pigAggFunction.star == star ) {
                return pigAggFunction;
            }
        }
        throw new IllegalArgumentException( "Pig agg func for " + polyphenyDb + " is not supported" );
    }
}

