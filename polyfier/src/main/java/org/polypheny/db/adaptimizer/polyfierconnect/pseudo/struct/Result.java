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

package org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.nodes.*;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.util.Pair;

import javax.swing.text.html.Option;
import java.util.*;
import java.util.stream.Collectors;

@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Result {
    Node node;
    CatalogSchema schema;
    String name;
    Alias alias;
    ArrayList<Column> columns;

    public Optional<Alias> getAlias() {
        if ( this.alias == null ) {
            return Optional.empty();
        }
        return Optional.of( this.alias );
    }

    public static Result from( Scan scan, CatalogTable table ) {
        Catalog catalog = Catalog.getInstance();
        Result result = new Result();
        result.setNode( scan );
        result.setSchema( table.getSchema() );
        result.setAlias( null );
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
        result.setAlias( Alias.create() );
        result.setColumns( (ArrayList<Column>) target.left.getResult().getColumns()
                .stream()
                .map(column -> Column.from( result, column ))
                .collect(Collectors.toList())
        );
        return result;
    }

    public static Result from( Join join, Pair<Column, Column> joinTargets ) {
        Result result = new Result();
        result.setNode( join );
        result.setSchema( joinTargets.left.getResult().getSchema() );
        joinTargets.left.getResult().getAlias().ifPresent( result::setAlias );
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
                columnsLeft = new ArrayList<>( join.getTarget().left.getResult().getColumns() );
                columnsLeft.addAll( columnsRight );
                result.setColumns( columnsLeft );
                break;
            case RIGHT:
                columnsLeft = (ArrayList<Column>) join.getTarget().left.getResult().getColumns()
                        .stream()
                        .filter( column -> ! column.equals( join.getJoinTarget().left ) )
                        .map( column -> Column.from( result, column ) )
                        .collect( Collectors.toList() );
                columnsRight = new ArrayList<>( join.getTarget().right.getResult().getColumns() );
                columnsRight.addAll( columnsLeft );
                result.setColumns( columnsRight );
                break;
        }

        result.setNode( join );

        return result;

    }

    public static Result from( Sort sort, Node target ) {
        Result result = new Result();
        result.setSchema( target.getResult().getSchema() );
        result.setName( target.getResult().getName() );
        target.getResult().getAlias().ifPresent( result::setAlias );
        result.setNode( sort );
        result.setColumns(
                (ArrayList<Column>) target.getResult().getColumns()
                        .stream()
                        .map(column -> Column.from( result, column ) )
                        .collect(Collectors.toList())
        );
        return result;
    }

    public static Result from( Filter filter ) {
        Result result = new Result();
        Node target = filter.getTarget();
        result.setSchema( target.getResult().getSchema() );
        result.setName( target.getResult().getName() );
        target.getResult().getAlias().ifPresent( result::setAlias );
        result.setNode( filter );
        result.setColumns(
                (ArrayList<Column>) target.getResult().getColumns()
                        .stream()
                        .map(column -> Column.from( result, column ) )
                        .collect(Collectors.toList())
        );
        return result;
    }

    public static Result from( Project project, List<Column> target ) {
        Result result = new Result();
        result.setSchema( target.get( 0 ).getResult().getSchema() );
        result.setName( target.get( 0 ).getResult().getName() );
        target.get( 0 ).getResult().getAlias().ifPresent( result::setAlias );
        result.setNode( project );
        result.setColumns(
                (ArrayList<Column>) target.get( 0 ).getResult().getColumns()
                        .stream()
                        .filter( target::contains )
                        .map(column -> Column.from( result, column ) )
                        .collect(Collectors.toList())
        );
        return result;
    }

}
