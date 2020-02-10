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

package ch.unibas.dmi.dbis.polyphenydb.interpreter;


import ch.unibas.dmi.dbis.polyphenydb.DataContext;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.runtime.ArrayBindable;
import ch.unibas.dmi.dbis.polyphenydb.runtime.Bindable;
import org.apache.calcite.linq4j.Enumerable;


/**
 * Utilities relating to {@link ch.unibas.dmi.dbis.polyphenydb.interpreter.Interpreter} and {@link ch.unibas.dmi.dbis.polyphenydb.interpreter.InterpretableConvention}.
 */
public class Interpreters {

    private Interpreters() {
    }


    /**
     * Creates a {@link Bindable} that interprets a given relational expression.
     */
    public static ArrayBindable bindable( final RelNode rel ) {
        if ( rel instanceof ArrayBindable ) {
            // E.g. if rel instanceof BindableRel
            return (ArrayBindable) rel;
        }
        return new ArrayBindable() {
            @Override
            public Enumerable<Object[]> bind( DataContext dataContext ) {
                return new Interpreter( dataContext, rel );
            }


            @Override
            public Class<Object[]> getElementType() {
                return Object[].class;
            }
        };
    }
}

