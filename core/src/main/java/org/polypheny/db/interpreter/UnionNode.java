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

package org.polypheny.db.interpreter;


import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.Set;
import org.polypheny.db.algebra.core.Union;


/**
 * Interpreter node that implements a {@link org.polypheny.db.algebra.core.Union}.
 */
public class UnionNode implements Node {

    private final ImmutableList<Source> sources;
    private final Sink sink;
    private final Union alg;


    public UnionNode( Compiler compiler, Union alg ) {
        ImmutableList.Builder<Source> builder = ImmutableList.builder();
        for ( int i = 0; i < alg.getInputs().size(); i++ ) {
            builder.add( compiler.source( alg, i ) );
        }
        this.sources = builder.build();
        this.sink = compiler.sink( alg );
        this.alg = alg;
    }


    @Override
    public void run() throws InterruptedException {
        final Set<Row> rows = alg.all ? null : new HashSet<>();
        for ( Source source : sources ) {
            Row row;
            while ( (row = source.receive()) != null ) {
                if ( rows == null || rows.add( row ) ) {
                    sink.send( row );
                }
            }
        }
    }

}

