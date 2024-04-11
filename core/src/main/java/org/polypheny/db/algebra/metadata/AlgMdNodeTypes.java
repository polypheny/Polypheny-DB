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


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Intersect;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Minus;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.SemiJoin;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.plan.hep.HepAlgVertex;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Util;


/**
 * RelMdNodeTypeCount supplies a default implementation of {@link AlgMetadataQuery#getNodeTypes} for the standard logical algebra.
 */
public class AlgMdNodeTypes implements MetadataHandler<BuiltInMetadata.NodeTypes> {

    public static final AlgMetadataProvider SOURCE =
            ReflectiveAlgMetadataProvider.reflectiveSource(
                    new AlgMdNodeTypes(),
                    BuiltInMethod.NODE_TYPES.method );


    @Override
    public MetadataDef<BuiltInMetadata.NodeTypes> getDef() {
        return BuiltInMetadata.NodeTypes.DEF;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.NodeTypes#getNodeTypes()}, invoked using reflection.
     *
     * @see AlgMetadataQuery#getNodeTypes(AlgNode)
     */
    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( AlgNode alg, AlgMetadataQuery mq ) {
        return getNodeTypes( alg, AlgNode.class, mq );
    }


    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( HepAlgVertex alg, AlgMetadataQuery mq ) {
        return mq.getNodeTypes( alg.getCurrentAlg() );
    }


    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( AlgSubset alg, AlgMetadataQuery mq ) {
        return mq.getNodeTypes( Util.first( alg.getBest(), alg.getOriginal() ) );
    }


    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( Union alg, AlgMetadataQuery mq ) {
        return getNodeTypes( alg, Union.class, mq );
    }


    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( Intersect alg, AlgMetadataQuery mq ) {
        return getNodeTypes( alg, Intersect.class, mq );
    }


    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( Minus alg, AlgMetadataQuery mq ) {
        return getNodeTypes( alg, Minus.class, mq );
    }


    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( Filter alg, AlgMetadataQuery mq ) {
        return getNodeTypes( alg, Filter.class, mq );
    }


    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( Calc alg, AlgMetadataQuery mq ) {
        return getNodeTypes( alg, Calc.class, mq );
    }


    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( Project alg, AlgMetadataQuery mq ) {
        return getNodeTypes( alg, Project.class, mq );
    }


    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( Sort alg, AlgMetadataQuery mq ) {
        return getNodeTypes( alg, Sort.class, mq );
    }


    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( Join alg, AlgMetadataQuery mq ) {
        return getNodeTypes( alg, Join.class, mq );
    }


    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( SemiJoin alg, AlgMetadataQuery mq ) {
        return getNodeTypes( alg, SemiJoin.class, mq );
    }


    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( Aggregate alg, AlgMetadataQuery mq ) {
        return getNodeTypes( alg, Aggregate.class, mq );
    }


    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( RelScan alg, AlgMetadataQuery mq ) {
        return getNodeTypes( alg, RelScan.class, mq );
    }


    public Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( Values alg, AlgMetadataQuery mq ) {
        return getNodeTypes( alg, Values.class, mq );
    }


    private static Multimap<Class<? extends AlgNode>, AlgNode> getNodeTypes( AlgNode alg, Class<? extends AlgNode> c, AlgMetadataQuery mq ) {
        final Multimap<Class<? extends AlgNode>, AlgNode> nodeTypeCount = ArrayListMultimap.create();
        for ( AlgNode input : alg.getInputs() ) {
            Multimap<Class<? extends AlgNode>, AlgNode> partialNodeTypeCount = mq.getNodeTypes( input );
            if ( partialNodeTypeCount == null ) {
                return null;
            }
            nodeTypeCount.putAll( partialNodeTypeCount );
        }
        nodeTypeCount.put( c, alg );
        return nodeTypeCount;
    }

}
