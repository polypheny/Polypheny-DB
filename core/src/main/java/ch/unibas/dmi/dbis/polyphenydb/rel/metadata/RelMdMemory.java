/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.metadata;


import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;


/**
 * Default implementations of the {@link ch.unibas.dmi.dbis.polyphenydb.rel.metadata.BuiltInMetadata.Memory} metadata provider for the standard logical algebra.
 *
 * @see RelMetadataQuery#isPhaseTransition
 * @see RelMetadataQuery#splitCount
 */
public class RelMdMemory implements MetadataHandler<BuiltInMetadata.Memory> {

    /**
     * Source for {@link ch.unibas.dmi.dbis.polyphenydb.rel.metadata.BuiltInMetadata.Memory}.
     */
    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    new RelMdMemory(),
                    BuiltInMethod.MEMORY.method,
                    BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE.method,
                    BuiltInMethod.CUMULATIVE_MEMORY_WITHIN_PHASE_SPLIT.method );


    protected RelMdMemory() {
    }


    public MetadataDef<BuiltInMetadata.Memory> getDef() {
        return BuiltInMetadata.Memory.DEF;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.Memory#memory()}, invoked using reflection.
     *
     * @see ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery#memory
     */
    public Double memory( RelNode rel, RelMetadataQuery mq ) {
        return null;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.Memory#cumulativeMemoryWithinPhase()}, invoked using reflection.
     *
     * @see ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery#memory
     */
    public Double cumulativeMemoryWithinPhase( RelNode rel, RelMetadataQuery mq ) {
        Double nullable = mq.memory( rel );
        if ( nullable == null ) {
            return null;
        }
        Boolean isPhaseTransition = mq.isPhaseTransition( rel );
        if ( isPhaseTransition == null ) {
            return null;
        }
        double d = nullable;
        if ( !isPhaseTransition ) {
            for ( RelNode input : rel.getInputs() ) {
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
     * @see ch.unibas.dmi.dbis.polyphenydb.rel.metadata.RelMetadataQuery#cumulativeMemoryWithinPhaseSplit
     */
    public Double cumulativeMemoryWithinPhaseSplit( RelNode rel, RelMetadataQuery mq ) {
        final Double memoryWithinPhase = mq.cumulativeMemoryWithinPhase( rel );
        final Integer splitCount = mq.splitCount( rel );
        if ( memoryWithinPhase == null || splitCount == null ) {
            return null;
        }
        return memoryWithinPhase / splitCount;
    }
}
