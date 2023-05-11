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

package org.polypheny.db.type.entity.graph;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyString;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.util.BuiltInMethod;

public class PolyDictionary extends PolyMap<PolyString, PolyValue> {

    private static Gson gson = new GsonBuilder().serializeNulls().enableComplexMapKeySerialization().create();


    public PolyDictionary( Map<PolyString, PolyValue> map ) {
        super( map );
    }


    public PolyDictionary() {
        this( Map.of() );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyDictionary.class, Expressions.call(
                BuiltInMethod.MAP_OF_ENTRIES.method,
                EnumUtils.expressionList(
                        entrySet()
                                .stream()
                                .map( p -> Expressions.call(
                                        BuiltInMethod.PAIR_OF.method,
                                        Expressions.constant( p.getKey(), String.class ),
                                        EnumUtils.getExpression( p.getValue(), Object.class ) ) ).collect( Collectors.toList() ) ) ) );
    }


    public static PolyDictionary fromString( String json ) {
        return gson.fromJson( json, PolyDictionary.class );
    }


}
