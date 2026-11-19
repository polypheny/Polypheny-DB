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

package org.polypheny.db.webui;


import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.logical.relational.LogicalRelMappingTransform;
import org.polypheny.db.algebra.mapping.ConversionColumnTransformer;
import org.polypheny.db.algebra.mapping.MappingScanBuilder;
import org.polypheny.db.algebra.rules.MappingTransformRule;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.Like;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.ScalarConversions;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.webui.models.SortDirection;
import org.polypheny.db.webui.models.requests.UIRequest;


/**
 * Reads a mapped table for Show Data, evaluating conversions outside the strict query plan.
 */
final class MappedTablePreview {

    private MappedTablePreview() {
    }


    static Page read( Statement statement, LogicalTable table, UIRequest request, int pageSize ) {
        var transaction = statement.getTransaction();
        statement.getQueryProcessor().lock( transaction );
        var snapshot = transaction.getSnapshot();
        var allocation = snapshot.alloc().getFromLogical( table.id ).stream().findFirst().orElseThrow();
        var builder = AlgBuilder.create( statement );
        var scans = new MappingScanBuilder( snapshot, () -> AlgBuilder.create( statement, builder.getCluster() ) );
        List<String> fields = allocation.getTupleType().getFieldNames();
        List<Predicate<List<PolyValue>>> filters = filters( fields, request );
        Comparator<PreviewRow> order = order( fields, request );
        long offset = request.noLimit ? 0 : (long) (Math.max( request.currentPage, 1 ) - 1) * pageSize;
        long end = request.noLimit ? Long.MAX_VALUE : offset + pageSize;
        List<PreviewRow> selected = new ArrayList<>();
        PriorityQueue<PreviewRow> sorted = order == null ? null : new PriorityQueue<>( order.reversed() );
        long matches = 0;
        for ( LogicalRelMappingTransform branch : scans.mappedBranches( allocation ) ) {
            var projects = MappingTransformRule.REL_INSTANCE.projectFields( branch );
            // Compile the physical scan alone. Each returned row is still in the input order of these expressions.
            var scan = statement.getQueryProcessor().prepareQuery( AlgRoot.of( branch.getInput(), Kind.SELECT ), true, false );
            try ( var iterator = scan.execute( statement, pageSize ) ) {
                for ( var rows = iterator.getNextBatch(); !rows.isEmpty(); rows = iterator.getNextBatch() ) {
                    for ( var row : rows ) {
                        boolean onPage = matches >= offset && matches < end;
                        if ( !onPage && filters.isEmpty() && sorted == null ) {
                            matches++;
                            continue;
                        }
                        var preview = convertRow( row, projects, statement.getDataContext(), matches );
                        if ( !filters.stream().allMatch( filter -> filter.test( preview.values() ) ) ) {
                            continue;
                        }
                        matches++;
                        if ( sorted != null ) {
                            sorted.add( preview );
                            if ( sorted.size() > end ) {
                                sorted.remove();
                            }
                        } else if ( onPage ) {
                            selected.add( preview );
                        }
                    }
                }
            }
        }
        if ( sorted != null ) {
            selected = sorted.stream().sorted( order ).skip( offset ).toList();
        }
        Map<Integer, Map<Integer, String>> errors = new HashMap<>();
        for ( int row = 0; row < selected.size(); row++ ) {
            if ( !selected.get( row ).errors().isEmpty() ) {
                errors.put( row, selected.get( row ).errors() );
            }
        }
        return new Page( selected.stream().map( PreviewRow::values ).toList(), errors, matches );
    }


    private static PreviewRow convertRow( List<PolyValue> row, List<RexNode> projects, DataContext context, long ordinal ) {
        List<PolyValue> values = new ArrayList<>( projects.size() );
        Map<Integer, String> errors = new HashMap<>();
        for ( var project : projects ) {
            if ( project instanceof RexIndexRef direct ) {
                values.add( row.get( direct.getIndex() ) );
            } else if ( project instanceof RexCall call && call.getKind() == Kind.MAPPING_CONVERT ) {
                var source = (RexIndexRef) call.getOperands().get( 0 );
                var name = ((RexLiteral) call.getOperands().get( 1 )).getValue().asString();
                var value = row.get( source.getIndex() );
                try {
                    values.add( ConversionColumnTransformer.convert( value, name, source.getType(), call.getType(), context ) );
                } catch ( GenericRuntimeException e ) {
                    errors.put( values.size(), conversionError( value, e ) );
                    values.add( null );
                }
            } else {
                throw new GenericRuntimeException( "Unsupported expression in mapped table preview: %s", project );
            }
        }
        return new PreviewRow( values, errors, ordinal );
    }


    private static String conversionError( PolyValue value, GenericRuntimeException error ) {
        if ( ScalarConversions.isNull( value ) ) {
            return error.getMessage();
        }
        var cause = error.getCause();
        String reason = cause instanceof NumberFormatException ? "The value is not a valid number." : cause == null ? null : cause.getMessage();
        String source = value.isString() ? value.asString().toQuotedJson() : value.toJson();
        return error.getMessage() + " Source value " + abbreviate( source ) + (reason == null ? "" : " · " + abbreviate( reason ));
    }


    private static String abbreviate( String text ) {
        return text != null && text.length() > 160 ? text.substring( 0, 160 ) + "…" : text;
    }


    private static List<Predicate<List<PolyValue>>> filters( List<String> fields, UIRequest request ) {
        List<Predicate<List<PolyValue>>> filters = new ArrayList<>();
        if ( request.filter == null ) {
            return filters;
        }
        for ( var entry : request.filter.entrySet() ) {
            int index = fields.indexOf( entry.getKey() );
            String value = entry.getValue();
            if ( index < 0 || value == null || value.isEmpty() ) {
                continue;
            }
            if ( value.startsWith( "[" ) ) {
                final String array;
                try {
                    array = HttpServer.mapper.readTree( value ).toString();
                } catch ( JsonProcessingException e ) {
                    throw new GenericRuntimeException( "Invalid array filter for column '%s'.", e, entry.getKey() );
                }
                filters.add( row -> !ScalarConversions.isNull( row.get( index ) ) && array.equals( row.get( index ).toJson() ) );
            } else {
                var pattern = Pattern.compile( Like.sqlToRegexLike( value + "%", null ) );
                filters.add( row -> {
                    var cell = row.get( index );
                    return !ScalarConversions.isNull( cell ) && pattern.matcher( cell.isString() ? cell.asString().value : cell.toJson() ).matches();
                } );
            }
        }
        return filters;
    }


    private static Comparator<PreviewRow> order( List<String> fields, UIRequest request ) {
        Comparator<PreviewRow> order = null;
        if ( request.sortState != null ) {
            for ( var entry : request.sortState.entrySet() ) {
                int index = fields.indexOf( entry.getKey() );
                if ( index < 0 || entry.getValue() == null || !entry.getValue().sorting ) {
                    continue;
                }
                Comparator<PreviewRow> field = Comparator.comparing( row -> {
                            var value = row.values().get( index );
                            return ScalarConversions.isNull( value ) ? null : value;
                        },
                        Comparator.nullsLast( Comparator.naturalOrder() ) );
                if ( entry.getValue().direction == SortDirection.DESC ) {
                    field = field.reversed();
                }
                order = order == null ? field : order.thenComparing( field );
            }
        }
        return order == null ? null : order.thenComparingLong( PreviewRow::ordinal );
    }


    private record PreviewRow( List<PolyValue> values, Map<Integer, String> errors, long ordinal ) {

    }


    record Page( List<List<PolyValue>> rows, Map<Integer, Map<Integer, String>> errors, long rowCount ) {

    }

}
