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


import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Interpreter node that implements a {@link org.polypheny.db.algebra.core.Project}.
 */
public class ProjectNode extends AbstractSingleNode<Project> {

    private final Scalar scalar;
    private final Context<PolyValue> context;
    private final int projectCount;


    public ProjectNode( Compiler compiler, Project alg ) {
        super( compiler, alg );
        this.projectCount = alg.getProjects().size();
        this.scalar = compiler.compile( alg.getProjects(), alg.getInput().getTupleType() );
        this.context = compiler.createContext();
    }


    @Override
    public void run() throws InterruptedException {
        Row<PolyValue> row;
        while ( (row = source.receive()) != null ) {
            context.values = row.getValues();
            PolyValue[] values = new PolyValue[projectCount];
            scalar.execute( context, values );
            sink.send( new Row<>( values, PolyValue.class ) );
        }
    }

}

