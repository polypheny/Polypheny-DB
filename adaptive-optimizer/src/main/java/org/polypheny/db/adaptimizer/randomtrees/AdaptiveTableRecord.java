/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adaptimizer.randomtrees;

import com.google.gson.annotations.SerializedName;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.adaptimizer.except.AdaptiveOptTreeGenException;
import org.polypheny.db.algebra.core.JoinAlgType;

@AllArgsConstructor
@Getter
@Setter
public class AdaptiveTableRecord implements TableRecord {

    @SerializedName( "name" )
    private String tableName;

    @SerializedName( "columns" )
    private List<String> columns;

    public static AdaptiveTableRecord from( AdaptiveTableRecord a ) {
        return new AdaptiveTableRecord(
            a.getTableName(),
            List.copyOf( a.getColumns() )
        );
    }

    public static AdaptiveTableRecord from( AdaptiveTableRecord a, String alias ) {
        return new AdaptiveTableRecord(
                alias,
                List.copyOf( a.getColumns() ).stream().map( str -> {

                    int i = str.indexOf( "___" );

                    if ( i == -1 ) {
                        return alias + "___" + str;
                    }

                    int j = str.indexOf( "." );

                    return alias + "___" + str.substring( i + 3, j ) + str.substring( j );

                } ).collect( Collectors.toList())
        );
    }


    @Override
    public String getTableName() {
        return this.tableName;
    }


    @Override
    public List<String> getColumns() {
        return this.columns;
    }


    @Override
    public void setColumns( List<String> columns ) {
        this.columns = columns;
    }

    public static AdaptiveTableRecord union( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        return AdaptiveTableRecord.from( left );
    }

    public static AdaptiveTableRecord intersect( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        return AdaptiveTableRecord.from( left );
    }

    public static AdaptiveTableRecord minus( AdaptiveTableRecord left, AdaptiveTableRecord right ) {
        return AdaptiveTableRecord.from( left );
    }

    public static AdaptiveTableRecord join( AdaptiveTableRecord left, AdaptiveTableRecord right, String joinL, String joinR, JoinAlgType joinAlgType ) {

        List<String> listL = new ArrayList<>( left.getColumns() );
        List<String> listR = new ArrayList<>( right.getColumns() );

        switch ( joinAlgType ) {
            case INNER:
                return joinInner( left.getTableName(), listL, listR, joinL, joinR);
            case LEFT:
                return joinLeft( left.getTableName(), listL, listR, joinL);
            case RIGHT:
                return joinRight( right.getTableName(), listL, listR, joinR);
            case FULL:
                return joinFull( left.getTableName(), listL, listR);
            default:
                throw new AdaptiveOptTreeGenException( "Invalid Join Type" + joinAlgType.name(), new IllegalArgumentException() );
        }
    }

    private static AdaptiveTableRecord joinLeft( String tableName, List<String> listL, List<String> listR, String joinR ) {
        listR.remove( joinR );
        listL.addAll( listR );
        return new AdaptiveTableRecord( tableName, listL );
    }
    private static AdaptiveTableRecord joinRight( String tableName, List<String> listL, List<String> listR, String joinL) {
        listL.remove( joinL );
        listR.addAll( listL );
        return new AdaptiveTableRecord( tableName, listR );
    }
    private static AdaptiveTableRecord joinInner( String tableName, List<String> listL, List<String> listR, String joinL, String joinR ) {
        listL.remove( joinL );
        listR.remove( joinR );
        listL.addAll( listR );
        return new AdaptiveTableRecord( tableName, listL );
    }
    private static AdaptiveTableRecord joinFull( String tableName, List<String> listL, List<String> listR) {
        listL.addAll( listR );
        return new AdaptiveTableRecord( tableName, listR );
    }

    public static AdaptiveTableRecord project( String tableName, List<String> columns ) {
        return new AdaptiveTableRecord( tableName, columns );
    }

    public boolean hasColumnsOfSameType( AdaptiveTableRecord other ) {
        return this.columns.equals( other.getColumns() );
    }


}
