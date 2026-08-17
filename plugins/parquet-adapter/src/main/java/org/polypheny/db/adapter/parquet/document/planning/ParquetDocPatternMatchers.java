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

package org.polypheny.db.adapter.parquet.document.planning;

import static org.polypheny.db.plan.AlgOptRule.none;
import static org.polypheny.db.plan.AlgOptRule.operand;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.parquet.document.execution.ParquetDocFilterTranslator;
import org.polypheny.db.adapter.parquet.relational.planning.EnumerableParquet;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.optimization.PatternMatcher;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.enumerable.EnumerableAggregate;
import org.polypheny.db.algebra.enumerable.EnumerableCalc;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.EnumerableProject;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilderFactory;
import org.polypheny.db.type.entity.PolyValue;

public final class ParquetDocPatternMatchers {

    private static final ParquetDocFilterTranslator translator = new ParquetDocFilterTranslator();


    private ParquetDocPatternMatchers() {
    }


    private static EnumerableParquet toEnumerable( AlgNode source, AlgNode input ) {
        return new EnumerableParquet(
                source.getCluster(),
                input.getTraitSet().replace( EnumerableConvention.INSTANCE ).replace( ModelTrait.DOCUMENT ),
                input );
    }


    /**
     * Pushes supported document Calc filters into a Parquet document scan.
     */
    public static PatternMatcher attachFiltersToScanUnderCalc(ParquetDocConvention out, AlgBuilderFactory factory ) {
        return new PatternMatcher(
                out,
                factory,
                operand(
                        EnumerableCalc.class,
                        operand( EnumerableParquet.class, operand( ParquetDocScan.class, none() ) )
                ),
                "attachDocumentFiltersToScanUnderCalc",
                call -> {
                    EnumerableCalc calc = call.alg( 0 );
                    ParquetDocScan scan = call.alg( 2 );
                    ParquetDocScan updatedScan = applyFilters( calc, scan );
                    if ( updatedScan == null ) {
                        return;
                    }
                    if ( calc.getProgram().projectsOnlyIdentity() ) {
                        call.transformTo( toEnumerable( calc, updatedScan ) );
                    } else {
                        call.transformTo( EnumerableCalc.create( toEnumerable( calc, updatedScan ), calc.getProgram() ) );
                    }
                }
        );
    }


    /**
     * Replaces supported field-independent aggregates directly over a Parquet document scan.
     */
    public static PatternMatcher aggregateOnScan(ParquetDocConvention out, AlgBuilderFactory factory ) {
        return new PatternMatcher(
                out,
                factory,
                operand(
                        EnumerableAggregate.class,
                        operand( EnumerableParquet.class, operand( ParquetDocScan.class, none() ) )
                ),
                "documentAggregateOnScan",
                call -> {
                    EnumerableAggregate aggregate = call.alg( 0 );
                    ParquetDocScan scan = call.alg( 2 );
                    if ( aggregateNeedsProjectedFields( aggregate ) ) {
                        return;
                    }
                    transformAggregate( call, aggregate, scan, new int[0] );
                }
        );
    }


    /**
     * Replaces supported aggregates over document field projections with a Parquet document aggregate.
     */
    public static PatternMatcher aggregateOnProjectScan(ParquetDocConvention out, AlgBuilderFactory factory ) {
        return new PatternMatcher(
                out,
                factory,
                operand(
                        EnumerableAggregate.class,
                        operand(
                                EnumerableProject.class,
                                operand( EnumerableParquet.class, operand( ParquetDocScan.class, none() ) )
                        )
                ),
                "documentAggregateOnProjectScan",
                call -> {
                    EnumerableAggregate aggregate = call.alg( 0 );
                    EnumerableProject project = call.alg( 1 );
                    ParquetDocScan scan = call.alg( 3 );
                    int[] fields = documentFields( project.getProjects(), scan );
                    if ( fields == null && (aggregateNeedsProjectedFields( aggregate ) || !isDocumentRootProjection( project.getProjects() )) ) {
                        return;
                    }
                    if ( fields == null ) {
                        fields = new int[0];
                    }
                    transformAggregate( call, aggregate, scan, fields );
                }
        );
    }


    /**
     * Replaces supported aggregates over project-only document Calcs with a Parquet document aggregate.
     */
    public static PatternMatcher aggregateOnCalcScan(ParquetDocConvention out, AlgBuilderFactory factory ) {
        return new PatternMatcher(
                out,
                factory,
                operand(
                        EnumerableAggregate.class,
                        operand(
                                EnumerableCalc.class,
                                operand( EnumerableParquet.class, operand( ParquetDocScan.class, none() ) )
                        )
                ),
                "documentAggregateOnCalcScan",
                call -> {
                    EnumerableAggregate aggregate = call.alg( 0 );
                    EnumerableCalc calc = call.alg( 1 );
                    ParquetDocScan scan = call.alg( 3 );
                    ParquetDocScan aggregateScan = scan;
                    if ( calc.getProgram().getCondition() != null ) {
                        aggregateScan = applyFilters( calc, scan );
                    }
                    if ( aggregateScan == null ) {
                        return;
                    }
                    List<RexNode> projects = calc.getProgram().getProjectList().stream()
                            .map( calc.getProgram()::expandLocalRef )
                            .toList();
                    int[] fields = documentFields( projects, aggregateScan );
                    if ( fields == null && (aggregateNeedsProjectedFields( aggregate ) || !isDocumentRootProjection( projects )) ) {
                        return;
                    }
                    if ( fields == null ) {
                        fields = new int[0];
                    }
                    transformAggregate( call, aggregate, aggregateScan, fields );
                }
        );
    }


    static ParquetDocScan applyFilters( EnumerableCalc calc, ParquetDocScan scan ) {
        RexProgram program = calc.getProgram();
        if ( program.getCondition() == null ) {
            return null;
        }

        List<ExportedColumn> columns = scan.getEntity().getParquetSource().getExportedColumns().get( scan.getEntity().name );
        if ( columns == null ) {
            return null;
        }

        List<RexNode> predicates = new ArrayList<>();
        AlgOptUtil.decomposeConjunction( program.expandLocalRef( program.getCondition() ), predicates );
        if ( predicates.isEmpty() ) {
            return null;
        }

        List<ParquetAdapterFilter<PolyValue>> filters = new ArrayList<>();
        for ( RexNode predicate : predicates ) {
            ParquetAdapterFilter<PolyValue> filter = translator.translate( columns, predicate );
            if ( filter == null ) {
                return null;
            }
            filters.add( filter );
        }

        if ( new HashSet<>( scan.getFilters() ).containsAll( filters ) ) {
            return null;
        }

        return scan.withFilters( filters );
    }


    private static void transformAggregate( AlgOptRuleCall call, Aggregate aggregate, ParquetDocScan scan, int[] fields ) {
        ParquetDocAggregate parquetAggregate = ParquetDocAggregate.create( scan, aggregate, fields );
        if ( parquetAggregate != null ) {
            call.transformTo( toEnumerable( aggregate, parquetAggregate ) );
        }
    }


    private static boolean aggregateNeedsProjectedFields( Aggregate aggregate ) {
        if ( !aggregate.getGroupSet().isEmpty() ) {
            return true;
        }
        return aggregate.getAggCallList().stream().anyMatch( aggregateCall -> !isDocumentRootCount( aggregateCall ) );
    }


    private static boolean isDocumentRootCount( org.polypheny.db.algebra.core.AggregateCall aggregateCall ) {
        if ( aggregateCall.getAggregation().getKind() != Kind.COUNT ) {
            return false;
        }
        return aggregateCall.getArgList().isEmpty()
                || (aggregateCall.getArgList().size() == 1 && aggregateCall.getArgList().get( 0 ) == 0);
    }


    private static boolean isDocumentRootProjection( List<? extends RexNode> projects ) {
        return projects.size() == 1
                && projects.get( 0 ) instanceof RexIndexRef indexRef
                && indexRef.getIndex() == 0;
    }


    private static int[] documentFields( List<? extends RexNode> projects, ParquetDocScan scan ) {
        List<ExportedColumn> columns = scan.getEntity().getParquetSource().getExportedColumns().get( scan.getEntity().name );
        if ( columns == null ) {
            return null;
        }
        int[] fields = new int[projects.size()];
        for ( int i = 0; i < projects.size(); i++ ) {
            String fieldName = documentFieldName( projects.get( i ) );
            if ( fieldName == null ) {
                return null;
            }
            ExportedColumn column = columns.stream()
                    .filter( candidate -> candidate.name().equalsIgnoreCase( fieldName ) )
                    .findFirst()
                    .orElse( null );
            if ( column == null ) {
                return null;
            }
            fields[i] = column.physicalPosition();
        }
        return fields;
    }


    private static String documentFieldName( RexNode node ) {
        while ( node.isA( Kind.CAST ) ) {
            node = ((RexCall) node).getOperands().get( 0 );
        }
        if ( !(node instanceof RexCall call) || call.getKind() != Kind.MQL_QUERY_VALUE || call.getOperands().size() != 2 ) {
            return null;
        }
        if ( !(call.getOperands().get( 0 ) instanceof RexIndexRef indexRef) || indexRef.getIndex() != 0 ) {
            return null;
        }

        RexNode path = call.getOperands().get( 1 );
        String foldedPath = foldedSingleFieldPath( path );
        if ( foldedPath != null ) {
            return foldedPath;
        }
        if ( !(path instanceof RexCall pathCall) || pathCall.getKind() != Kind.ARRAY_VALUE_CONSTRUCTOR || pathCall.getOperands().size() != 1 ) {
            return null;
        }
        RexNode element = pathCall.getOperands().get( 0 );
        if ( !(element instanceof RexLiteral literal) || literal.getValue() == null || !literal.getValue().isString() ) {
            return null;
        }
        return literal.getValue().asString().value;
    }


    private static String foldedSingleFieldPath( RexNode path ) {
        if ( !(path instanceof RexLiteral literal) || literal.getValue() == null || !literal.getValue().isList() ) {
            return null;
        }
        List<? extends PolyValue> elements = literal.getValue().asList();
        if ( elements.size() != 1 || !elements.get( 0 ).isString() ) {
            return null;
        }
        return elements.get( 0 ).asString().value;
    }

}
