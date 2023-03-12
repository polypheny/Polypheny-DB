/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.polyfier.core.construct.model;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.polypheny.db.polyfier.core.construct.nodes.*;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.util.Pair;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Result {
    Node node;
    CatalogSchema schema;
    String name;
    ArrayList<Column> columns;

    public static Result from( Scan scan, CatalogTable table ) {
        Catalog catalog = Catalog.getInstance();
        Result result = new Result();
        result.setNode( scan );
        result.setSchema( table.getSchema() );
        result.setColumns(
                (ArrayList<Column>) table.fieldIds.stream().map(catalog::getColumn).map(
                        column -> Column.from( result, column )
                ).collect(Collectors.toList())
        );
        result.setName( table.name );
        return result;
    }

    public static Result from( SetNode setOp, Pair<Node, Node> target ) {
        Result result = new Result();
        result.setNode( setOp );
        result.setSchema( target.left.getResult().getSchema() );
        result.setName( target.left.getResult().getName() );

        final String alias = "As" + UUID.randomUUID().toString().substring( 0, 4 );

        setOp.setAlias( alias );

        result.setColumns( (ArrayList<Column>) target.left.getResult().getColumns()
                .stream()
                .map(column -> Column.from( result, column ))
                .collect(Collectors.toList())
        );

        result.getColumns().forEach( column -> {
            column.getAliases().add( alias );
        });

        return result;
    }

    public static Result from( Join join, Pair<Column, Column> joinTargets ) {
        Result result = new Result();
        result.setNode( join );
        result.setSchema( joinTargets.left.getResult().getSchema() );
        result.setName( joinTargets.left.getResult().getName() );

        ArrayList<Column> columnsLeft, columnsRight;
        switch ( join.getJoinType() ) {
            case INNER: // ~
            case FULL:  // ~
            case LEFT:
                columnsRight = (ArrayList<Column>) join.getTarget().right.getResult().getColumns()
                        .stream()
                        .filter( column -> ! column.equals( join.getJoinTarget().right ) )
                        .map( column -> Column.from( result, column ) )
                        .collect( Collectors.toList() );
                columnsLeft = (ArrayList<Column>) join.getTarget().left.getResult().getColumns()
                        .stream()
                        .map(column -> Column.from( result, column ) )
                        .collect(Collectors.toList());
                columnsLeft.addAll( columnsRight );
                result.setColumns( columnsLeft );
                break;
            case RIGHT:
                columnsLeft = (ArrayList<Column>) join.getTarget().left.getResult().getColumns()
                        .stream()
                        .filter( column -> ! column.equals( join.getJoinTarget().left ) )
                        .map( column -> Column.from( result, column ) )
                        .collect( Collectors.toList() );
                columnsRight = (ArrayList<Column>) join.getTarget().right.getResult().getColumns()
                        .stream()
                        .map(column -> Column.from( result, column ) )
                        .collect(Collectors.toList());
                columnsRight.addAll( columnsLeft );
                result.setColumns( columnsRight );
                break;
        }

        result.setNode( join );

        return result;
    }

    private static Result from( Node posterior, Result prior ) {
        Result result = new Result();
        result.setSchema( prior.getSchema() );
        result.setName( prior.getName() );
        result.setNode( posterior );
        return result;
    }

    private static void mapFields( Result posterior, Result prior, @Nullable List<Column> targeted ) {
        Stream<Column> columnStream = prior.getColumns().stream();
        if ( targeted != null ) {
            columnStream = columnStream.filter( targeted::contains );
        }
        posterior.setColumns(
                (ArrayList<Column>) columnStream.map(column -> Column.from( posterior, column ) ).collect(Collectors.toList())
        );
    }

    private static Result fromSimpleUnary( Unary unary ) {
        Result result = from( unary, unary.getTarget().getResult() );
        mapFields( result, unary.getTarget().getResult(), null );
        return result;
    }

    public static Result from( Sort sort ) {
        return fromSimpleUnary( sort );
    }

    public static Result from( Filter filter ) {
        return fromSimpleUnary( filter );
    }

    public static Result from( Project project, List<Column> targeted ) {
        Result result = from( project, project.getTarget().getResult() );
        mapFields( result, project.getTarget().getResult(), targeted );
        return result;
    }


}
