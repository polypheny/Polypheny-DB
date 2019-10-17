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


import ch.unibas.dmi.dbis.polyphenydb.plan.hep.HepRelVertex;
import ch.unibas.dmi.dbis.polyphenydb.plan.volcano.RelSubset;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Aggregate;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Exchange;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Filter;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Join;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Sort;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableScan;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.Union;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexTableInputRef.RelTableRef;
import ch.unibas.dmi.dbis.polyphenydb.util.BuiltInMethod;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Default implementation of {@link RelMetadataQuery#getTableReferences} for the standard logical algebra.
 *
 * The goal of this provider is to return all tables used by a given expression identified uniquely by a {@link RelTableRef}.
 *
 * Each unique identifier {@link RelTableRef} of a table will equal to the identifier obtained running {@link RelMdExpressionLineage} over the same plan node for an expression that refers to the same table.
 *
 * If tables cannot be obtained, we return null.
 */
public class RelMdTableReferences implements MetadataHandler<BuiltInMetadata.TableReferences> {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource( BuiltInMethod.TABLE_REFERENCES.method, new RelMdTableReferences() );


    protected RelMdTableReferences() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.TableReferences> getDef() {
        return BuiltInMetadata.TableReferences.DEF;
    }


    // Catch-all rule when none of the others apply.
    public Set<RelTableRef> getTableReferences( RelNode rel, RelMetadataQuery mq ) {
        return null;
    }


    public Set<RelTableRef> getTableReferences( HepRelVertex rel, RelMetadataQuery mq ) {
        return mq.getTableReferences( rel.getCurrentRel() );
    }


    public Set<RelTableRef> getTableReferences( RelSubset rel, RelMetadataQuery mq ) {
        return mq.getTableReferences( Util.first( rel.getBest(), rel.getOriginal() ) );
    }


    /**
     * TableScan table reference.
     */
    public Set<RelTableRef> getTableReferences( TableScan rel, RelMetadataQuery mq ) {
        return ImmutableSet.of( RelTableRef.of( rel.getTable(), 0 ) );
    }


    /**
     * Table references from Aggregate.
     */
    public Set<RelTableRef> getTableReferences( Aggregate rel, RelMetadataQuery mq ) {
        return mq.getTableReferences( rel.getInput() );
    }


    /**
     * Table references from Join.
     */
    public Set<RelTableRef> getTableReferences( Join rel, RelMetadataQuery mq ) {
        final RelNode leftInput = rel.getLeft();
        final RelNode rightInput = rel.getRight();
        final Set<RelTableRef> result = new HashSet<>();

        // Gather table references, left input references remain unchanged
        final Multimap<List<String>, RelTableRef> leftQualifiedNamesToRefs = HashMultimap.create();
        final Set<RelTableRef> leftTableRefs = mq.getTableReferences( leftInput );
        if ( leftTableRefs == null ) {
            // We could not infer the table refs from left input
            return null;
        }
        for ( RelTableRef leftRef : leftTableRefs ) {
            assert !result.contains( leftRef );
            result.add( leftRef );
            leftQualifiedNamesToRefs.put( leftRef.getQualifiedName(), leftRef );
        }

        // Gather table references, right input references might need to be updated if there are table names clashes with left input
        final Set<RelTableRef> rightTableRefs = mq.getTableReferences( rightInput );
        if ( rightTableRefs == null ) {
            // We could not infer the table refs from right input
            return null;
        }
        for ( RelTableRef rightRef : rightTableRefs ) {
            int shift = 0;
            Collection<RelTableRef> lRefs = leftQualifiedNamesToRefs.get( rightRef.getQualifiedName() );
            if ( lRefs != null ) {
                shift = lRefs.size();
            }
            RelTableRef shiftTableRef = RelTableRef.of( rightRef.getTable(), shift + rightRef.getEntityNumber() );
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
    public Set<RelTableRef> getTableReferences( Union rel, RelMetadataQuery mq ) {
        final Set<RelTableRef> result = new HashSet<>();

        // Infer column origin expressions for given references
        final Multimap<List<String>, RelTableRef> qualifiedNamesToRefs = HashMultimap.create();
        for ( RelNode input : rel.getInputs() ) {
            final Map<RelTableRef, RelTableRef> currentTablesMapping = new HashMap<>();
            final Set<RelTableRef> inputTableRefs = mq.getTableReferences( input );
            if ( inputTableRefs == null ) {
                // We could not infer the table refs from input
                return null;
            }
            for ( RelTableRef tableRef : inputTableRefs ) {
                int shift = 0;
                Collection<RelTableRef> lRefs = qualifiedNamesToRefs.get( tableRef.getQualifiedName() );
                if ( lRefs != null ) {
                    shift = lRefs.size();
                }
                RelTableRef shiftTableRef = RelTableRef.of( tableRef.getTable(), shift + tableRef.getEntityNumber() );
                assert !result.contains( shiftTableRef );
                result.add( shiftTableRef );
                currentTablesMapping.put( tableRef, shiftTableRef );
            }
            // Add to existing qualified names
            for ( RelTableRef newRef : currentTablesMapping.values() ) {
                qualifiedNamesToRefs.put( newRef.getQualifiedName(), newRef );
            }
        }

        // Return result
        return result;
    }


    /**
     * Table references from Project.
     */
    public Set<RelTableRef> getTableReferences( Project rel, final RelMetadataQuery mq ) {
        return mq.getTableReferences( rel.getInput() );
    }


    /**
     * Table references from Filter.
     */
    public Set<RelTableRef> getTableReferences( Filter rel, RelMetadataQuery mq ) {
        return mq.getTableReferences( rel.getInput() );
    }


    /**
     * Table references from Sort.
     */
    public Set<RelTableRef> getTableReferences( Sort rel, RelMetadataQuery mq ) {
        return mq.getTableReferences( rel.getInput() );
    }


    /**
     * Table references from Exchange.
     */
    public Set<RelTableRef> getTableReferences( Exchange rel, RelMetadataQuery mq ) {
        return mq.getTableReferences( rel.getInput() );
    }
}

