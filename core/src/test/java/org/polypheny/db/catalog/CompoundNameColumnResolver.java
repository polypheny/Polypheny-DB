/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.catalog;


import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.StructKind;
import org.polypheny.db.util.Pair;


/**
 * ColumnResolver implementation that resolves CompoundNameColumn by simulating Phoenix behaviors.
 */
final class CompoundNameColumnResolver implements MockCatalogReader.ColumnResolver {

    private final Map<String, Integer> nameMap = new HashMap<>();
    private final Map<String, Map<String, Integer>> groupMap = new HashMap<>();
    private final String defaultColumnGroup;


    CompoundNameColumnResolver( List<CompoundNameColumn> columns, String defaultColumnGroup ) {
        this.defaultColumnGroup = defaultColumnGroup;
        for ( Ord<CompoundNameColumn> column : Ord.zip( columns ) ) {
            nameMap.put( column.e.getName(), column.i );
            Map<String, Integer> subMap = groupMap.computeIfAbsent( column.e.first, k -> new HashMap<>() );
            subMap.put( column.e.second, column.i );
        }
    }


    @Override
    public List<Pair<AlgDataTypeField, List<String>>> resolveColumn( AlgDataType rowType, AlgDataTypeFactory typeFactory, List<String> names ) {
        List<Pair<AlgDataTypeField, List<String>>> ret = new ArrayList<>();
        if ( names.size() >= 2 ) {
            Map<String, Integer> subMap = groupMap.get( names.get( 0 ) );
            if ( subMap != null ) {
                Integer index = subMap.get( names.get( 1 ) );
                if ( index != null ) {
                    ret.add( new Pair<>( rowType.getFieldList().get( index ), names.subList( 2, names.size() ) ) );
                }
            }
        }

        final String columnName = names.get( 0 );
        final List<String> remainder = names.subList( 1, names.size() );
        Integer index = nameMap.get( columnName );
        if ( index != null ) {
            ret.add( new Pair<>( rowType.getFieldList().get( index ), remainder ) );
            return ret;
        }

        final List<String> priorityGroups = Arrays.asList( "", defaultColumnGroup );
        for ( String group : priorityGroups ) {
            Map<String, Integer> subMap = groupMap.get( group );
            if ( subMap != null ) {
                index = subMap.get( columnName );
                if ( index != null ) {
                    ret.add( new Pair<>( rowType.getFieldList().get( index ), remainder ) );
                    return ret;
                }
            }
        }
        for ( Map.Entry<String, Map<String, Integer>> entry : groupMap.entrySet() ) {
            if ( priorityGroups.contains( entry.getKey() ) ) {
                continue;
            }
            index = entry.getValue().get( columnName );
            if ( index != null ) {
                ret.add( new Pair<>( rowType.getFieldList().get( index ), remainder ) );
            }
        }

        if ( ret.isEmpty() && names.size() == 1 ) {
            Map<String, Integer> subMap = groupMap.get( columnName );
            if ( subMap != null ) {
                List<Map.Entry<String, Integer>> entries = new ArrayList<>( subMap.entrySet() );
                entries.sort( ( o1, o2 ) -> o1.getValue() - o2.getValue() );
                ret.add( new Pair<>( new AlgDataTypeFieldImpl( columnName, -1, createStructType( rowType, typeFactory, entries ) ), remainder ) );
            }
        }

        return ret;
    }


    private static AlgDataType createStructType( final AlgDataType rowType, AlgDataTypeFactory typeFactory, final List<Map.Entry<String, Integer>> entries ) {
        return typeFactory.createStructType(
                StructKind.PEEK_FIELDS,
                new AbstractList<AlgDataType>() {
                    @Override
                    public AlgDataType get( int index ) {
                        final int i = entries.get( index ).getValue();
                        return rowType.getFieldList().get( i ).getType();
                    }


                    @Override
                    public int size() {
                        return entries.size();
                    }
                },
                new AbstractList<String>() {
                    @Override
                    public String get( int index ) {
                        return entries.get( index ).getKey();
                    }


                    @Override
                    public int size() {
                        return entries.size();
                    }
                } );
    }

}

