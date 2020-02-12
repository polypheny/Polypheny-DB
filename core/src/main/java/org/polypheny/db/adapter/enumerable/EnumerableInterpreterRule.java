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

package org.polypheny.db.adapter.enumerable;


import org.polypheny.db.interpreter.BindableConvention;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterRule;
import org.polypheny.db.rel.core.RelFactories;
import org.polypheny.db.tools.RelBuilderFactory;
import java.util.function.Predicate;


/**
 * Planner rule that converts {@link org.polypheny.db.interpreter.BindableRel} to {@link org.polypheny.db.adapter.enumerable.EnumerableRel} by creating
 * an {@link org.polypheny.db.adapter.enumerable.EnumerableInterpreter}.
 */
public class EnumerableInterpreterRule extends ConverterRule {

    public static final EnumerableInterpreterRule INSTANCE = new EnumerableInterpreterRule( RelFactories.LOGICAL_BUILDER );


    /**
     * Creates an EnumerableInterpreterRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public EnumerableInterpreterRule( RelBuilderFactory relBuilderFactory ) {
        super( RelNode.class, (Predicate<RelNode>) r -> true, BindableConvention.INSTANCE, EnumerableConvention.INSTANCE, relBuilderFactory, "EnumerableInterpreterRule" );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        return EnumerableInterpreter.create( rel, 0.5d );
    }
}
