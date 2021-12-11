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

package org.polypheny.db.algebra.metadata;


import org.polypheny.db.algebra.AlgNode;


/**
 * Source of metadata about relational expressions.
 *
 * The metadata is typically various kinds of statistics used to estimate costs.
 *
 * Each kind of metadata has an interface that extends {@link Metadata} and has a method. Some examples: {@link BuiltInMetadata.Selectivity}, {@link BuiltInMetadata.ColumnUniqueness}.
 */
public interface MetadataFactory {

    /**
     * Returns a metadata interface to get a particular kind of metadata from a particular relational expression. Returns null if that kind of metadata is not available.
     *
     * @param <M> Metadata type
     * @param alg Relational expression
     * @param mq Metadata query
     * @param metadataClazz Metadata class
     * @return Metadata bound to {@code rel} and {@code query}
     */
    <M extends Metadata> M query( AlgNode alg, AlgMetadataQuery mq, Class<M> metadataClazz );

}

