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

package org.polypheny.db.plan.hep;


import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.lang.reflect.Method;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.metadata.Metadata;
import org.polypheny.db.rel.metadata.MetadataDef;
import org.polypheny.db.rel.metadata.MetadataHandler;
import org.polypheny.db.rel.metadata.RelMetadataProvider;
import org.polypheny.db.rel.metadata.UnboundMetadata;


/**
 * HepRelMetadataProvider implements the {@link RelMetadataProvider} interface by combining metadata from the rels inside of a {@link HepRelVertex}.
 */
class HepRelMetadataProvider implements RelMetadataProvider {

    @Override
    public boolean equals( Object obj ) {
        return obj instanceof HepRelMetadataProvider;
    }


    @Override
    public int hashCode() {
        return 107;
    }


    @Override
    public <M extends Metadata> UnboundMetadata<M> apply( Class<? extends RelNode> relClass, final Class<? extends M> metadataClass ) {
        return ( rel, mq ) -> {
            if ( !(rel instanceof HepRelVertex) ) {
                return null;
            }
            HepRelVertex vertex = (HepRelVertex) rel;
            final RelNode rel2 = vertex.getCurrentRel();
            UnboundMetadata<M> function = rel.getCluster().getMetadataProvider().apply( rel2.getClass(), metadataClass );
            return function.bind( rel2, mq );
        };
    }


    @Override
    public <M extends Metadata> Multimap<Method, MetadataHandler<M>> handlers( MetadataDef<M> def ) {
        return ImmutableMultimap.of();
    }
}

