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
import org.polypheny.db.util.BuiltInMethod;


/**
 * Default implementations of the {@link org.polypheny.db.algebra.metadata.BuiltInMetadata.Memory} metadata provider for the standard logical algebra.
 *
 * @see AlgMetadataQuery#isPhaseTransition
 * @see AlgMetadataQuery#splitCount
 */
public class AlgMdMemory implements MetadataHandler<BuiltInMetadata.Memory> {

    /**
     * Source for {@link org.polypheny.db.algebra.metadata.BuiltInMetadata.Memory}.
     */
    public static final AlgMetadataProvider SOURCE =
            ReflectiveAlgMetadataProvider.reflectiveSource(
                    new AlgMdMemory(),
                    BuiltInMethod.MEMORY.method,
                    BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE.method,
                    BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE_SPLIT.method );


    protected AlgMdMemory() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.Memory> getDef() {
        return BuiltInMetadata.Memory.DEF;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.Memory#memory()}, invoked using reflection.
     *
     * @see AlgMetadataQuery#memory
     */
    public Double memory( AlgNode alg, AlgMetadataQuery mq ) {
        return null;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.Memory#cumulativeMemoryWithinPhase()}, invoked using reflection.
     *
     * @see AlgMetadataQuery#memory
     */
    public Double cumulativeMemoryWithinPhase( AlgNode alg, AlgMetadataQuery mq ) {
        Double nullable = mq.memory( alg );
        if ( nullable == null ) {
            return null;
        }
        Boolean isPhaseTransition = mq.isPhaseTransition( alg );
        if ( isPhaseTransition == null ) {
            return null;
        }
        double d = nullable;
        if ( !isPhaseTransition ) {
            for ( AlgNode input : alg.getInputs() ) {
                nullable = mq.cumulativeMemoryWithinPhase( input );
                if ( nullable == null ) {
                    return null;
                }
                d += nullable;
            }
        }
        return d;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.Memory#cumulativeMemoryWithinPhaseSplit()}, invoked using reflection.
     *
     * @see AlgMetadataQuery#cumulativeMemoryWithinPhaseSplit
     */
    public Double cumulativeMemoryWithinPhaseSplit( AlgNode alg, AlgMetadataQuery mq ) {
        final Double memoryWithinPhase = mq.cumulativeMemoryWithinPhase( alg );
        final Integer splitCount = mq.splitCount( alg );
        if ( memoryWithinPhase == null || splitCount == null ) {
            return null;
        }
        return memoryWithinPhase / splitCount;
    }

}
