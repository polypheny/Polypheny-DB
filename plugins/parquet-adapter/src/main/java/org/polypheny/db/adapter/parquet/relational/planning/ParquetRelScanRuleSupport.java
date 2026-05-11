/*
 * Copyright 2019-2026 The Polypheny Project
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
 */

package org.polypheny.db.adapter.parquet.relational.planning;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Calc;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.type.PolyType;

/**
 * Class handles direct Parquet scans, scans hidden inside planner subsets,
 * and simple projection-only Calc nodes above scans, while keeping the projected field mapping correct.
 * This lets join and filter rules understand which real Parquet table and physical columns are being used.
 */
public final class ParquetRelScanRuleSupport {

    private ParquetRelScanRuleSupport() {
    }

    /*
    Returns the node directly if it is already ParquetRelScan, converts a generic relational scan
    over ParquetRelTable into ParquetRelScan, and also looks inside AlgSubset alternatives.
     */
    static ParquetRelScan findDirectRelScan( AlgNode alg ) {
        return findDirectRelScan( alg, newVisitedSet() );
    }


    private static ParquetRelScan findDirectRelScan( AlgNode alg, Set<AlgNode> visited ) {
        if ( alg == null || !visited.add( alg ) ) {
            return null;
        }
        ParquetRelScan directScan = directRelScan( alg );
        if ( directScan != null ) {
            return directScan;
        }
        if ( alg instanceof AlgSubset subset ) {
            ParquetRelScan relScan = findDirectRelScanMatchingRowType( subset.getBest(), subset, visited );
            if ( relScan != null ) {
                return relScan;
            }
            relScan = findDirectRelScanMatchingRowType( subset.getOriginal(), subset, visited );
            if ( relScan != null ) {
                return relScan;
            }
            for ( AlgNode candidate : subset.getAlgList() ) {
                relScan = findDirectRelScanMatchingRowType( candidate, subset, visited );
                if ( relScan != null ) {
                    return relScan;
                }
            }
        }
        return null;
    }


    /**
     * Tries to find a ParquetRelScan even when it is wrapped by a simple projection-only Calc.
     * It first checks for a direct scan, then unwraps Calc nodes if they only reorder/select fields
     * and do not filter or compute expressions. It returns a new ParquetRelScan with fields mapped to the original physical Parquet columns.
     */
    static ParquetRelScan findProjectedRelScan( AlgNode alg ) {
        return findProjectedRelScan( alg, newVisitedSet() );
    }


    private static ParquetRelScan findProjectedRelScan( AlgNode alg, Set<AlgNode> visited ) {
        if ( alg == null || !visited.add( alg ) ) {
            return null;
        }
        ParquetRelScan directScan = directRelScan( alg );
        if ( directScan != null ) {
            return directScan;
        }
        if ( alg instanceof Calc calc ) {
            ParquetRelScan inputScan = findProjectedRelScanMatchingRowType( calc.getInput(), calc.getInput(), visited );
            if ( inputScan == null ) {
                return null;
            }
            int[] projectedFields = projectedFields( calc.getProgram(), inputScan.getFields() );
            if ( projectedFields == null ) {
                return null;
            }
            return new ParquetRelScan( inputScan.getCluster(), inputScan.getEntity(), projectedFields, inputScan.getFilters() );
        }
        if ( alg instanceof Project project ) {
            ParquetRelScan inputScan = findProjectedRelScanMatchingRowType( project.getInput(), project.getInput(), visited );
            if ( inputScan == null ) {
                return null;
            }
            int[] projectedFields = projectedFields( project.getProjects(), inputScan.getFields() );
            if ( projectedFields == null ) {
                return null;
            }
            return new ParquetRelScan( inputScan.getCluster(), inputScan.getEntity(), projectedFields, inputScan.getFilters() );
        }
        if ( alg instanceof AlgSubset subset ) {
            ParquetRelScan relScan = findProjectedRelScanMatchingRowType( subset.getBest(), subset, visited );
            if ( relScan != null ) {
                return relScan;
            }
            relScan = findProjectedRelScanMatchingRowType( subset.getOriginal(), subset, visited );
            if ( relScan != null ) {
                return relScan;
            }
            for ( AlgNode candidate : subset.getAlgList() ) {
                relScan = findProjectedRelScanMatchingRowType( candidate, subset, visited );
                if ( relScan != null ) {
                    return relScan;
                }
            }
        }
        return null;
    }


    private static ParquetRelScan directRelScan( AlgNode alg ) {
        if ( alg instanceof ParquetRelScan relScan ) {
            return relScan;
        }
        // if ParquetRelTable was found - create new scan
        if ( alg instanceof RelScan<?> relScan && relScan.getEntity().unwrap( ParquetRelTable.class ).isPresent() ) {
            ParquetRelTable table = relScan.getEntity().unwrapOrThrow( ParquetRelTable.class );
            if (!AlgOptUtil.areRowTypesEqual( alg.getTupleType(), table.getTupleType(), false )) {
                return null;
            }
            return new ParquetRelScan( relScan.getCluster(), table, relScan.identity().stream().mapToInt( Integer::intValue ).toArray() );
        }
        return null;
    }


    private static ParquetRelScan findDirectRelScanMatchingRowType( AlgNode candidate, AlgNode expected, Set<AlgNode> visited ) {
        ParquetRelScan relScan = findDirectRelScan( candidate, branchVisitedSet( visited ) );
        return rowTypesMatch( expected, relScan ) ? relScan : null;
    }


    private static ParquetRelScan findProjectedRelScanMatchingRowType( AlgNode candidate, AlgNode expected, Set<AlgNode> visited ) {
        ParquetRelScan relScan = findProjectedRelScan( candidate, branchVisitedSet( visited ) );
        return rowTypesMatch( expected, relScan ) ? relScan : null;
    }


    private static Set<AlgNode> newVisitedSet() {
        return Collections.newSetFromMap( new IdentityHashMap<>() );
    }


    private static Set<AlgNode> branchVisitedSet( Set<AlgNode> visited ) {
        Set<AlgNode> branch = newVisitedSet();
        branch.addAll( visited );
        return branch;
    }


    static boolean rowTypesMatch( AlgNode expected, AlgNode actual ) {
        return expected != null && actual != null && AlgOptUtil.areRowTypesEqual( expected.getTupleType(), actual.getTupleType(), false );
    }


    /**
     * Maps the output columns of a projection-only RexProgram back to the underlying Parquet scan field indexes.
     */
    public static int[] projectedFields( RexProgram program, int[] inputFields ) {
        if ( program.getCondition() != null ) {
            return null;
        }

        List<RexNode> projects = program.getProjectList().stream()
                .map( program::expandLocalRef )
                .toList();
        return projectedFields( projects, inputFields );
    }


    public static int[] projectedFields( List<RexNode> projects, int[] inputFields ) {
        int[] projectedFields = new int[projects.size()];
        for ( int i = 0; i < projects.size(); i++ ) {
            RexNode project = projects.get( i );
            if ( !(project instanceof RexIndexRef indexRef) ) {
                return null;
            }
            int index = indexRef.getIndex();
            if ( index < 0 || index >= inputFields.length ) {
                return null;
            }
            projectedFields[i] = inputFields[index];
        }
        return projectedFields;
    }


    static List<PolyType> fieldTypes( AlgNode alg ) {
        return alg.getTupleType().getFields().stream()
                .map( field -> field.getType().getPolyType() )
                .toList();
    }

}
