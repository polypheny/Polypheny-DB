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

package org.polypheny.db.algebra.metadata;


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AlgPlanner;


/**
 * Metadata about an algebra expression.
 * <p>
 * For particular types of metadata, a sub-class defines one of more methods to query that metadata. Then a {@link AlgMetadataProvider} can offer those kinds of metadata for particular sub-classes of {@link AlgNode}.
 * <p>
 * User code (typically in a planner rule or an implementation of {@link AlgNode#computeSelfCost(AlgPlanner, AlgMetadataQuery)}) acquires a {@code Metadata} instance by calling {@link AlgNode#metadata}.
 * <p>
 * A {@code Metadata} instance already knows which particular {@code AlgNode} it is describing, so the methods do not pass in the {@code AlgNode}. In fact, quite a few metadata methods have no extra parameters. For instance, you can
 * get the row-count as follows:
 *
 * <blockquote><pre><code>
 * {@link AlgNode} alg;
 * double tupleCount = alg.metadata(TupleCount.class).rowCount();
 * </code></pre></blockquote>
 */
public interface Metadata {

    /**
     * Returns the algebra expression that this metadata is about.
     */
    AlgNode alg();

}

