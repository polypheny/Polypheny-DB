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


import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Context while converting a tree of {@link AlgNode} to a program that can be run by an {@link Interpreter}.
 */
public interface Compiler {

    /**
     * Compiles an expression to an executable form.
     */
    Scalar compile( List<RexNode> nodes, AlgDataType inputRowType );

    AlgDataType combinedRowType( List<AlgNode> inputs );

    Source<PolyValue> source( AlgNode alg, int ordinal );

    /**
     * Creates a Sink for a relational expression to write into.
     *
     * This method is generally called from the constructor of a {@link Node}. But a constructor could instead call {@link #enumerable(AlgNode, Enumerable)}.
     *
     * @param alg Relational expression
     * @return Sink
     */
    Sink sink( AlgNode alg );

    /**
     * Tells the interpreter that a given relational expression wishes to give its output as an enumerable.
     *
     * This is as opposed to the norm, where a relational expression calls {@link #sink(AlgNode)}, then its {@link Node#run()} method writes into that sink.
     *
     * @param alg Relational expression
     * @param rowEnumerable Contents of relational expression
     */
    void enumerable( AlgNode alg, Enumerable<Row<PolyValue>> rowEnumerable );

    DataContext getDataContext();

    Context<PolyValue> createContext();

}

