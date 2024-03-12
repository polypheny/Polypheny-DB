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


import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.Exchange;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Union;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.plan.hep.HepAlgVertex;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexTableIndexRef.AlgTableRef;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Util;


/**
 * Default implementation of {@link AlgMetadataQuery#getTableReferences} for the standard logical algebra.
 *
 * The goal of this provider is to return all tables used by a given expression identified uniquely by a {@link AlgTableRef}.
 *
 * Each unique identifier {@link AlgTableRef} of a table will equal to the identifier obtained running {@link AlgMdExpressionLineage} over the same plan node for an expression that refers to the same table.
 *
 * If tables cannot be obtained, we return null.
 */
public class AlgMdTableReferences implements MetadataHandler<BuiltInMetadata.TableReferences> {

    public static final AlgMetadataProvider SOURCE = ReflectiveAlgMetadataProvider.reflectiveSource( new AlgMdTableReferences(), BuiltInMethod.TABLE_REFERENCES.method );


    protected AlgMdTableReferences() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.TableReferences> getDef() {
        return BuiltInMetadata.TableReferences.DEF;
    }


    // Catch-all rule when none of the others apply.
    public Set<AlgTableRef> getTableReferences( AlgNode alg, AlgMetadataQuery mq ) {
        return null;
    }


    public Set<AlgTableRef> getTableReferences( HepAlgVertex alg, AlgMetadataQuery mq ) {
        return mq.getTableReferences( alg.getCurrentAlg() );
    }


    public Set<AlgTableRef> getTableReferences( AlgSubset alg, AlgMetadataQuery mq ) {
        return mq.getTableReferences( Util.first( alg.getBest(), alg.getOriginal() ) );
    }


    /**
     * Scan table reference.
     */
    public Set<AlgTableRef> getTableReferences( RelScan<?> alg, AlgMetadataQuery mq ) {
        return ImmutableSet.of( AlgTableRef.of( alg.getEntity(), 0 ) );
    }


    /**
     * Table references from Aggregate.
     */
    public Set<AlgTableRef> getTableReferences( Aggregate alg, AlgMetadataQuery mq ) {
        return mq.getTableReferences( alg.getInput() );
    }


    /**
     * Table references from Join.
     */
    public Set<AlgTableRef> getTableReferences( Join alg, AlgMetadataQuery mq ) {
        final AlgNode leftInput = alg.getLeft();
        final AlgNode rightInput = alg.getRight();
        final Set<AlgTableRef> result = new HashSet<>();

        // Gather table references, left input references remain unchanged
        final Multimap<List<String>, AlgTableRef> leftQualifiedNamesToRefs = HashMultimap.create();
        final Set<AlgTableRef> leftTableRefs = mq.getTableReferences( leftInput );
        if ( leftTableRefs == null ) {
            // We could not infer the table refs from left input
            return null;
        }
        for ( AlgTableRef leftRef : leftTableRefs ) {
            assert !result.contains( leftRef );
            result.add( leftRef );
            leftQualifiedNamesToRefs.put( leftRef.getQualifiedName(), leftRef );
        }

        // Gather table references, right input references might need to be updated if there are table names clashes with left input
        final Set<AlgTableRef> rightTableRefs = mq.getTableReferences( rightInput );
        if ( rightTableRefs == null ) {
            // We could not infer the table refs from right input
            return null;
        }
        for ( AlgTableRef rightRef : rightTableRefs ) {
            int shift = 0;
            Collection<AlgTableRef> lRefs = leftQualifiedNamesToRefs.get( rightRef.getQualifiedName() );
            if ( lRefs != null ) {
                shift = lRefs.size();
            }
            AlgTableRef shiftTableRef = AlgTableRef.of( rightRef.getTable(), shift + rightRef.getEntityNumber() );
            assert !result.contains( shiftTableRef );
            result.add( shiftTableRef );
        }

        // Return result
        return result;
    }


    /**
     * Table references from {@link Union}.
     *
     * For Union operator, we might be able to extract multiple table references.
     */
    public Set<AlgTableRef> getTableReferences( Union alg, AlgMetadataQuery mq ) {
        final Set<AlgTableRef> result = new HashSet<>();

        // Infer column origin expressions for given references
        final Multimap<List<String>, AlgTableRef> qualifiedNamesToRefs = HashMultimap.create();
        for ( AlgNode input : alg.getInputs() ) {
            final Map<AlgTableRef, AlgTableRef> currentTablesMapping = new HashMap<>();
            final Set<AlgTableRef> inputTableRefs = mq.getTableReferences( input );
            if ( inputTableRefs == null ) {
                // We could not infer the table refs from input
                return null;
            }
            for ( AlgTableRef tableRef : inputTableRefs ) {
                int shift = 0;
                Collection<AlgTableRef> lRefs = qualifiedNamesToRefs.get( tableRef.getQualifiedName() );
                if ( lRefs != null ) {
                    shift = lRefs.size();
                }
                AlgTableRef shiftTableRef = AlgTableRef.of( tableRef.getTable(), shift + tableRef.getEntityNumber() );
                assert !result.contains( shiftTableRef );
                result.add( shiftTableRef );
                currentTablesMapping.put( tableRef, shiftTableRef );
            }
            // Add to existing qualified names
            for ( AlgTableRef newRef : currentTablesMapping.values() ) {
                qualifiedNamesToRefs.put( newRef.getQualifiedName(), newRef );
            }
        }

        // Return result
        return result;
    }


    /**
     * Table references from Project.
     */
    public Set<AlgTableRef> getTableReferences( Project alg, final AlgMetadataQuery mq ) {
        return mq.getTableReferences( alg.getInput() );
    }


    /**
     * Table references from Filter.
     */
    public Set<AlgTableRef> getTableReferences( Filter alg, AlgMetadataQuery mq ) {
        return mq.getTableReferences( alg.getInput() );
    }


    /**
     * Table references from Sort.
     */
    public Set<AlgTableRef> getTableReferences( Sort alg, AlgMetadataQuery mq ) {
        return mq.getTableReferences( alg.getInput() );
    }


    /**
     * Table references from Exchange.
     */
    public Set<AlgTableRef> getTableReferences( Exchange alg, AlgMetadataQuery mq ) {
        return mq.getTableReferences( alg.getInput() );
    }

}

