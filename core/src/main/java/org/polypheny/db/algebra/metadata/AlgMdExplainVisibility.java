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
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.util.BuiltInMethod;


/**
 * RelMdExplainVisibility supplies a default implementation of {@link AlgMetadataQuery#isVisibleInExplain} for the standard logical algebra.
 */
public class AlgMdExplainVisibility implements MetadataHandler<BuiltInMetadata.ExplainVisibility> {

    public static final AlgMetadataProvider SOURCE = ReflectiveAlgMetadataProvider.reflectiveSource( new AlgMdExplainVisibility(), BuiltInMethod.EXPLAIN_VISIBILITY.method );


    private AlgMdExplainVisibility() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.ExplainVisibility> getDef() {
        return BuiltInMetadata.ExplainVisibility.DEF;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.ExplainVisibility#isVisibleInExplain(ExplainLevel)}, invoked using reflection.
     *
     * @see AlgMetadataQuery#isVisibleInExplain(AlgNode, ExplainLevel)
     */
    public Boolean isVisibleInExplain( AlgNode alg, AlgMetadataQuery mq, ExplainLevel explainLevel ) {
        // no information available
        return null;
    }

}

