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

package org.polypheny.db.interpreter;


import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;


/**
 * Relational expression that can implement itself using an interpreter.
 */
public interface InterpretableAlg extends AlgNode {

    /**
     * Creates an interpreter node to implement this relational expression.
     */
    Node implement( InterpreterImplementor implementor );

    /**
     * Context when a {@link AlgNode} is being converted to an interpreter {@link Node}.
     */
    class InterpreterImplementor {

        public final Compiler compiler;
        public final Map<String, Object> internalParameters = new LinkedHashMap<>();
        public final DataContext dataContext;
        public final Map<AlgNode, List<Sink>> algSinks = new HashMap<>();


        public InterpreterImplementor( Compiler compiler, DataContext dataContext ) {
            this.compiler = compiler;
            this.dataContext = dataContext;
        }

    }

}

