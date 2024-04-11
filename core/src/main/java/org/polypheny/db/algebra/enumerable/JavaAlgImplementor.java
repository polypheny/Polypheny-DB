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

package org.polypheny.db.algebra.enumerable;


import lombok.Getter;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.plan.AlgImplementor;
import org.polypheny.db.rex.RexBuilder;


/**
 * Abstract base class for implementations of {@link AlgImplementor} that generate java code.
 */
@Getter
public abstract class JavaAlgImplementor implements AlgImplementor {

    private final RexBuilder rexBuilder;


    public JavaAlgImplementor( RexBuilder rexBuilder ) {
        this.rexBuilder = rexBuilder;
        assert rexBuilder.getTypeFactory() instanceof JavaTypeFactory : "Type factory of rexBuilder should be a JavaTypeFactory";
    }


    public JavaTypeFactory getTypeFactory() {
        return (JavaTypeFactory) rexBuilder.getTypeFactory();
    }


    /**
     * Returns the expression used to access {@link DataContext}.
     *
     * @return expression used to access {@link DataContext}.
     */
    public ParameterExpression getRootExpression() {
        return DataContext.ROOT;
    }

}
