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


import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.Exchange;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.RelTableFunctionScan;
import org.polypheny.db.algebra.core.SetOp;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitor;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.util.BuiltInMethod;


/**
 * RelMdColumnOrigins supplies a default implementation of {@link AlgMetadataQuery#getColumnOrigins} for the standard logical algebra.
 */
public class AlgMdColumnOrigins implements MetadataHandler<BuiltInMetadata.ColumnOrigin> {

    public static final AlgMetadataProvider SOURCE = ReflectiveAlgMetadataProvider.reflectiveSource( new AlgMdColumnOrigins(), BuiltInMethod.COLUMN_ORIGIN.method );


    private AlgMdColumnOrigins() {
    }


    @Override
    public MetadataDef<BuiltInMetadata.ColumnOrigin> getDef() {
        return BuiltInMetadata.ColumnOrigin.DEF;
    }


    public Set<AlgColumnOrigin> getColumnOrigins( Aggregate alg, AlgMetadataQuery mq, int iOutputColumn ) {
        if ( iOutputColumn < alg.getGroupCount() ) {
            // Group columns pass through directly.
            return mq.getColumnOrigins( alg.getInput(), iOutputColumn );
        }

        if ( alg.indicator ) {
            if ( iOutputColumn < alg.getGroupCount() + alg.getIndicatorCount() ) {
                // The indicator column is originated here.
                return ImmutableSet.of();
            }
        }

        // Aggregate columns are derived from input columns
        AggregateCall call = alg.getAggCallList().get( iOutputColumn - alg.getGroupCount() - alg.getIndicatorCount() );

        final Set<AlgColumnOrigin> set = new HashSet<>();
        for ( Integer iInput : call.getArgList() ) {
            Set<AlgColumnOrigin> inputSet = mq.getColumnOrigins( alg.getInput(), iInput );
            inputSet = createDerivedColumnOrigins( inputSet );
            if ( inputSet != null ) {
                set.addAll( inputSet );
            }
        }
        return set;
    }


    public Set<AlgColumnOrigin> getColumnOrigins( Join alg, AlgMetadataQuery mq, int iOutputColumn ) {
        int nLeftColumns = alg.getLeft().getTupleType().getFields().size();
        Set<AlgColumnOrigin> set;
        boolean derived = false;
        if ( iOutputColumn < nLeftColumns ) {
            set = mq.getColumnOrigins( alg.getLeft(), iOutputColumn );
            if ( alg.getJoinType().generatesNullsOnLeft() ) {
                derived = true;
            }
        } else {
            set = mq.getColumnOrigins( alg.getRight(), iOutputColumn - nLeftColumns );
            if ( alg.getJoinType().generatesNullsOnRight() ) {
                derived = true;
            }
        }
        if ( derived ) {
            // nulls are generated due to outer join; that counts as derivation
            set = createDerivedColumnOrigins( set );
        }
        return set;
    }


    public Set<AlgColumnOrigin> getColumnOrigins( SetOp alg, AlgMetadataQuery mq, int iOutputColumn ) {
        final Set<AlgColumnOrigin> set = new HashSet<>();
        for ( AlgNode input : alg.getInputs() ) {
            Set<AlgColumnOrigin> inputSet = mq.getColumnOrigins( input, iOutputColumn );
            if ( inputSet == null ) {
                return null;
            }
            set.addAll( inputSet );
        }
        return set;
    }


    public Set<AlgColumnOrigin> getColumnOrigins( Project alg, final AlgMetadataQuery mq, int iOutputColumn ) {
        final AlgNode input = alg.getInput();
        RexNode rexNode = alg.getProjects().get( iOutputColumn );

        if ( rexNode instanceof RexIndexRef ) {
            // Direct reference:  no derivation added.
            RexIndexRef inputRef = (RexIndexRef) rexNode;
            return mq.getColumnOrigins( input, inputRef.getIndex() );
        }

        // Anything else is a derivation, possibly from multiple columns.
        final Set<AlgColumnOrigin> set = new HashSet<>();
        RexVisitor visitor =
                new RexVisitorImpl<Void>( true ) {
                    @Override
                    public Void visitIndexRef( RexIndexRef inputRef ) {
                        Set<AlgColumnOrigin> inputSet = mq.getColumnOrigins( input, inputRef.getIndex() );
                        if ( inputSet != null ) {
                            set.addAll( inputSet );
                        }
                        return null;
                    }
                };
        rexNode.accept( visitor );

        return createDerivedColumnOrigins( set );
    }


    public Set<AlgColumnOrigin> getColumnOrigins( Filter alg, AlgMetadataQuery mq, int iOutputColumn ) {
        return mq.getColumnOrigins( alg.getInput(), iOutputColumn );
    }


    public Set<AlgColumnOrigin> getColumnOrigins( Sort alg, AlgMetadataQuery mq, int iOutputColumn ) {
        return mq.getColumnOrigins( alg.getInput(), iOutputColumn );
    }


    public Set<AlgColumnOrigin> getColumnOrigins( Exchange alg, AlgMetadataQuery mq, int iOutputColumn ) {
        return mq.getColumnOrigins( alg.getInput(), iOutputColumn );
    }


    public Set<AlgColumnOrigin> getColumnOrigins( RelTableFunctionScan alg, AlgMetadataQuery mq, int iOutputColumn ) {
        final Set<AlgColumnOrigin> set = new HashSet<>();
        Set<AlgColumnMapping> mappings = alg.getColumnMappings();
        if ( mappings == null ) {
            if ( alg.getInputs().size() > 0 ) {
                // This is a non-leaf transformation:  say we don't know about origins, because there are probably columns below.
                return null;
            } else {
                // This is a leaf transformation: say there are fer sure no column origins.
                return set;
            }
        }
        for ( AlgColumnMapping mapping : mappings ) {
            if ( mapping.iOutputColumn != iOutputColumn ) {
                continue;
            }
            final AlgNode input = alg.getInputs().get( mapping.iInputRel );
            final int column = mapping.iInputColumn;
            Set<AlgColumnOrigin> origins = mq.getColumnOrigins( input, column );
            if ( origins == null ) {
                return null;
            }
            if ( mapping.derived ) {
                origins = createDerivedColumnOrigins( origins );
            }
            set.addAll( origins );
        }
        return set;
    }


    // Catch-all rule when none of the others apply.
    public Set<AlgColumnOrigin> getColumnOrigins( AlgNode alg, AlgMetadataQuery mq, int iOutputColumn ) {
        // NOTE jvs 28-Mar-2006: We may get this wrong for a physical table expression which supports projections.  In that case, it's up to the plugin writer to override with the correct information.
        if ( alg.getInputs().size() > 0 ) {
            // No generic logic available for non-leaf rels.
            return null;
        }

        final Set<AlgColumnOrigin> set = new HashSet<>();

        Entity entity = alg.getEntity();
        if ( entity == null ) {
            // Somebody is making column values up out of thin air, like a VALUES clause, so we return an empty set.
            return set;
        }

        // Detect the case where a physical table expression is performing projection, and say we don't know instead of making any assumptions.
        // (Theoretically we could try to map the projection using column names.)  This detection assumes the table expression doesn't handle rename as well.
        if ( entity.getTupleType() != alg.getTupleType() ) {
            return null;
        }

        set.add( new AlgColumnOrigin( entity, iOutputColumn, false ) );
        return set;
    }


    private Set<AlgColumnOrigin> createDerivedColumnOrigins( Set<AlgColumnOrigin> inputSet ) {
        if ( inputSet == null ) {
            return null;
        }
        final Set<AlgColumnOrigin> set = new HashSet<>();
        for ( AlgColumnOrigin rco : inputSet ) {
            AlgColumnOrigin derived =
                    new AlgColumnOrigin(
                            rco.getOriginTable(),
                            rco.getOriginColumnOrdinal(),
                            true );
            set.add( derived );
        }
        return set;
    }

}

