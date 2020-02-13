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

package org.polypheny.db.rel.metadata;


import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.core.Exchange;
import org.polypheny.db.rel.core.TableScan;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.metadata.BuiltInMetadata.Parallelism;
import org.polypheny.db.util.BuiltInMethod;


/**
 * Default implementations of the {@link Parallelism} metadata provider for the standard logical algebra.
 *
 * @see org.polypheny.db.rel.metadata.RelMetadataQuery#isPhaseTransition
 * @see org.polypheny.db.rel.metadata.RelMetadataQuery#splitCount
 */
public class RelMdParallelism implements MetadataHandler<BuiltInMetadata.Parallelism> {

    /**
     * Source for {@link Parallelism}.
     */
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    new RelMdParallelism(),
                    BuiltInMethod.IS_PHASE_TRANSITION.method,
                    BuiltInMethod.SPLIT_COUNT.method );


    protected RelMdParallelism() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.Parallelism> getDef() {
        return BuiltInMetadata.Parallelism.DEF;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.Parallelism#isPhaseTransition()}, invoked using reflection.
     *
     * @see org.polypheny.db.rel.metadata.RelMetadataQuery#isPhaseTransition
     */
    public Boolean isPhaseTransition( RelNode rel, RelMetadataQuery mq ) {
        return false;
    }


    public Boolean isPhaseTransition( TableScan rel, RelMetadataQuery mq ) {
        return true;
    }


    public Boolean isPhaseTransition( Values rel, RelMetadataQuery mq ) {
        return true;
    }


    public Boolean isPhaseTransition( Exchange rel, RelMetadataQuery mq ) {
        return true;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.Parallelism#splitCount()}, invoked using reflection.
     *
     * @see org.polypheny.db.rel.metadata.RelMetadataQuery#splitCount
     */
    public Integer splitCount( RelNode rel, RelMetadataQuery mq ) {
        return 1;
    }
}

