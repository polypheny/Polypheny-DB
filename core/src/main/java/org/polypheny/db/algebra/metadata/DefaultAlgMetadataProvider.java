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


import com.google.common.collect.ImmutableList;


/**
 * {@link DefaultAlgMetadataProvider} supplies a default implementation of the {@link AlgMetadataProvider} interface. It provides generic formulas and derivation rules for the standard logical algebra;
 * coverage corresponds to the methods declared in {@link AlgMetadataQuery}.
 */
public class DefaultAlgMetadataProvider extends ChainedAlgMetadataProvider {

    public static final DefaultAlgMetadataProvider INSTANCE = new DefaultAlgMetadataProvider();


    /**
     * Creates a new default provider. This provider defines "catch-all" handlers for generic RelNodes, so it should always be given the lowest priority when chaining.
     *
     * Use this constructor only from a sub-class. Otherwise, use the singleton instance, {@link #INSTANCE}.
     */
    protected DefaultAlgMetadataProvider() {
        super(
                ImmutableList.of(
                        AlgMdPercentageOriginalRows.SOURCE,
                        AlgMdColumnOrigins.SOURCE,
                        AlgMdExpressionLineage.SOURCE,
                        AlgMdTableReferences.SOURCE,
                        AlgMdNodeTypes.SOURCE,
                        AlgMdTupleCount.SOURCE,
                        AlgMdMaxRowCount.SOURCE,
                        AlgMdMinRowCount.SOURCE,
                        AlgMdUniqueKeys.SOURCE,
                        AlgMdColumnUniqueness.SOURCE,
                        AlgMdPopulationSize.SOURCE,
                        AlgMdSize.SOURCE,
                        AlgMdParallelism.SOURCE,
                        AlgMdDistribution.SOURCE,
                        AlgMdMemory.SOURCE,
                        AlgMdDistinctRowCount.SOURCE,
                        AlgMdSelectivity.SOURCE,
                        AlgMdExplainVisibility.SOURCE,
                        AlgMdPredicates.SOURCE,
                        AlgMdAllPredicates.SOURCE,
                        AlgMdCollation.SOURCE ) );
    }

}

