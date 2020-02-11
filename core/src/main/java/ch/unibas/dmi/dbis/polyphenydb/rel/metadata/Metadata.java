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

package ch.unibas.dmi.dbis.polyphenydb.rel.metadata;


import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptPlanner;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;


/**
 * Metadata about a relational expression.
 *
 * For particular types of metadata, a sub-class defines one of more methods to query that metadata. Then a {@link RelMetadataProvider} can offer those kinds of metadata for particular sub-classes of {@link RelNode}.
 *
 * User code (typically in a planner rule or an implementation of {@link RelNode#computeSelfCost(RelOptPlanner, RelMetadataQuery)}) acquires a {@code Metadata} instance by calling {@link RelNode#metadata}.
 *
 * A {@code Metadata} instance already knows which particular {@code RelNode} it is describing, so the methods do not pass in the {@code RelNode}. In fact, quite a few metadata methods have no extra parameters. For instance, you can
 * get the row-count as follows:
 *
 * <blockquote><pre><code>
 * RelNode rel;
 * double rowCount = rel.metadata(RowCount.class).rowCount();
 * </code></pre></blockquote>
 */
public interface Metadata {

    /**
     * Returns the relational expression that this metadata is about.
     */
    RelNode rel();
}

