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

package org.polypheny.db.plan;


import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.util.Wrapper;


/**
 * Node in a planner.
 */
public interface AlgOptNode extends Wrapper {

    /**
     * Returns the ID of this algebra expression, unique among all algebra expressions created since the server was started.
     *
     * @return Unique ID
     */
    int getId();

    /**
     * Returns a string which concisely describes the definition of this algebra expression. Two algebra expressions are equivalent if and
     * only if their digests are the same.
     * <p>
     * The digest does not contain the algebra expression's identity -- that would prevent similar algebra expressions from ever comparing
     * equal -- but does include the identity of children (on the assumption that children have already been normalized).
     * <p>
     * If you want a descriptive string which contains the identity, call {@link Object#toString()}, which always returns "rel#{id}:{digest}".
     *
     * @return Digest of this {@code AlgNode}
     */
    String getDigest();

    /**
     * Retrieves this AlgNode's traits. Note that although the RelTraitSet returned is modifiable, it <b>must not</b> be modified during
     * optimization. It is legal to modify the traits of a {@link AlgNode} before or after optimization, although doing so could render a tree of RelNodes
     * unimplementable. If a AlgNode's traits need to be modified during optimization, clone the {@link AlgNode} and change the clone's traits.
     *
     * @return this AlgNode's trait set
     */
    AlgTraitSet getTraitSet();


    AlgDataType getTupleType();

    /**
     * Returns a string which describes the algebra expression and, unlike {@link #getDigest()}, also includes the identity. Typically returns
     * "rel#{id}:{digest}".
     *
     * @return String which describes the algebra expression and, unlike {@link #getDigest()}, also includes the identity
     */
    String getDescription();

    /**
     * Returns an array of this algebra expression's inputs. If there are no inputs, returns an empty list, not {@code null}.
     *
     * @return this algebra expression's inputs
     */
    List<? extends AlgOptNode> getInputs();

    /**
     * Returns the cluster this algebra expression belongs to.
     *
     * @return cluster
     */
    AlgCluster getCluster();


}

