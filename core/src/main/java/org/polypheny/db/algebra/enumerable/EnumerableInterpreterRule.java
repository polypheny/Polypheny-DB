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


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.interpreter.BindableAlg;
import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.tools.AlgBuilderFactory;


/**
 * Planner rule that converts {@link BindableAlg} to {@link EnumerableAlg} by creating
 * an {@link EnumerableInterpreter}.
 */
public class EnumerableInterpreterRule extends ConverterRule {

    public static final EnumerableInterpreterRule INSTANCE = new EnumerableInterpreterRule( AlgFactories.LOGICAL_BUILDER );


    /**
     * Creates an EnumerableInterpreterRule.
     *
     * @param algBuilderFactory Builder for algebra expressions
     */
    public EnumerableInterpreterRule( AlgBuilderFactory algBuilderFactory ) {
        super( AlgNode.class, r -> true, BindableConvention.INSTANCE, EnumerableConvention.INSTANCE, algBuilderFactory, "EnumerableInterpreterRule" );
    }


    @Override
    public AlgNode convert( AlgNode alg ) {
        return EnumerableInterpreter.create( alg, 0.5d );
    }

}
