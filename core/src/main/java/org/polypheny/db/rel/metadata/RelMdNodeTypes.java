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


import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepRelVertex;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.RelSubset;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Calc;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Intersect;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Minus;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.SemiJoin;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Union;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Values;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;


/**
 * RelMdNodeTypeCount supplies a default implementation of {@link RelMetadataQuery#getNodeTypes} for the standard logical algebra.
 */
public class RelMdNodeTypes implements MetadataHandler<BuiltInMetadata.NodeTypes> {

    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.NODE_TYPES.method,
                    new RelMdNodeTypes() );


    @Override
    public MetadataDef<BuiltInMetadata.NodeTypes> getDef() {
        return BuiltInMetadata.NodeTypes.DEF;
    }


    /**
     * Catch-all implementation for {@link BuiltInMetadata.NodeTypes#getNodeTypes()}, invoked using reflection.
     *
     * @see RelMetadataQuery#getNodeTypes(RelNode)
     */
    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( RelNode rel, RelMetadataQuery mq ) {
        return getNodeTypes( rel, RelNode.class, mq );
    }


    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( HepRelVertex rel, RelMetadataQuery mq ) {
        return mq.getNodeTypes( rel.getCurrentRel() );
    }


    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( RelSubset rel, RelMetadataQuery mq ) {
        return mq.getNodeTypes( Util.first( rel.getBest(), rel.getOriginal() ) );
    }


    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( Union rel, RelMetadataQuery mq ) {
        return getNodeTypes( rel, Union.class, mq );
    }


    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( Intersect rel, RelMetadataQuery mq ) {
        return getNodeTypes( rel, Intersect.class, mq );
    }


    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( Minus rel, RelMetadataQuery mq ) {
        return getNodeTypes( rel, Minus.class, mq );
    }


    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( Filter rel, RelMetadataQuery mq ) {
        return getNodeTypes( rel, Filter.class, mq );
    }


    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( Calc rel, RelMetadataQuery mq ) {
        return getNodeTypes( rel, Calc.class, mq );
    }


    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( Project rel, RelMetadataQuery mq ) {
        return getNodeTypes( rel, Project.class, mq );
    }


    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( Sort rel, RelMetadataQuery mq ) {
        return getNodeTypes( rel, Sort.class, mq );
    }


    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( Join rel, RelMetadataQuery mq ) {
        return getNodeTypes( rel, Join.class, mq );
    }


    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( SemiJoin rel, RelMetadataQuery mq ) {
        return getNodeTypes( rel, SemiJoin.class, mq );
    }


    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( Aggregate rel, RelMetadataQuery mq ) {
        return getNodeTypes( rel, Aggregate.class, mq );
    }


    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( TableScan rel, RelMetadataQuery mq ) {
        return getNodeTypes( rel, TableScan.class, mq );
    }


    public Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( Values rel, RelMetadataQuery mq ) {
        return getNodeTypes( rel, Values.class, mq );
    }


    private static Multimap<Class<? extends RelNode>, RelNode> getNodeTypes( RelNode rel, Class<? extends RelNode> c, RelMetadataQuery mq ) {
        final Multimap<Class<? extends RelNode>, RelNode> nodeTypeCount = ArrayListMultimap.create();
        for ( RelNode input : rel.getInputs() ) {
            Multimap<Class<? extends RelNode>, RelNode> partialNodeTypeCount = mq.getNodeTypes( input );
            if ( partialNodeTypeCount == null ) {
                return null;
            }
            nodeTypeCount.putAll( partialNodeTypeCount );
        }
        nodeTypeCount.put( c, rel );
        return nodeTypeCount;
    }

}
