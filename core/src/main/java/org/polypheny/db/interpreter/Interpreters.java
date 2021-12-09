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


import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.runtime.ArrayBindable;
import org.polypheny.db.runtime.Bindable;


/**
 * Utilities relating to {@link org.polypheny.db.interpreter.Interpreter} and {@link org.polypheny.db.interpreter.InterpretableConvention}.
 */
public class Interpreters {

    private Interpreters() {
    }


    /**
     * Creates a {@link Bindable} that interprets a given relational expression.
     */
    public static ArrayBindable bindable( final AlgNode alg ) {
        if ( alg instanceof ArrayBindable ) {
            // E.g. if alg instanceof BindableRel
            return (ArrayBindable) alg;
        }
        return new ArrayBindable() {
            @Override
            public Enumerable<Object[]> bind( DataContext dataContext ) {
                return new Interpreter( dataContext, alg );
            }


            @Override
            public Class<Object[]> getElementType() {
                return Object[].class;
            }
        };
    }

}

