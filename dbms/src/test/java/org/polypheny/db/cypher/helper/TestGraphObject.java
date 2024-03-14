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
 */

package org.polypheny.db.cypher.helper;

import static org.polypheny.db.functions.Functions.toBigDecimal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.TestHelper;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.util.Pair;

public class TestGraphObject implements TestObject {

    public static double EPSILON = 0.2;

    @Nullable
    final PolyString id;

    @Nullable
    final Map<PolyString, PolyValue> properties;

    @Nullable
    final PolyList<PolyString> labels;


    public TestGraphObject( @Nullable String id, @Nullable Map<PolyString, PolyValue> properties, @Nullable List<PolyString> labels ) {
        this.id = PolyString.of( id );
        this.properties = properties;
        this.labels = PolyList.of( labels );
    }


    @NotNull
    public static Map<PolyString, PolyValue> getProps( Pair<String, Object>[] properties ) {
        Map<PolyString, PolyValue> props = new HashMap<>();

        for ( Pair<String, Object> property : properties ) {
            props.put( PolyString.of( property.left ), TestHelper.toPolyValue( property.right ) );
        }
        return props;
    }


    static List<PolyString> getLabels( List<String> labels ) {
        return labels.stream().map( PolyString::of ).toList();
    }


    @Override
    public boolean matches( PolyValue other, boolean exclusive ) {
        assert other instanceof GraphPropertyHolder;
        return matches( (GraphPropertyHolder) other, exclusive, true );
    }


    @SneakyThrows
    @Override
    public PolyValue toPoly( String val ) {
        return PolyValue.fromTypedJson( val, CypherTestTemplate.Type.from( this ).getPolyClass() );
    }


    public boolean matches( GraphPropertyHolder other, boolean exclusive, boolean ignoreId ) {
        boolean matches = true;
        if ( !ignoreId && id != null ) {
            matches = id.equals( other.id );
        }
        if ( properties != null ) {
            if ( exclusive ) {
                matches &= properties.size() == other.properties.size();
            }
            for ( Entry<PolyString, PolyValue> entry : properties.entrySet() ) {
                if ( other.properties.containsKey( entry.getKey() ) ) {
                    if ( entry.getValue().isList() ) {
                        int i = 0;
                        PolyList<PolyString> list = entry.getValue().asList();

                        for ( PolyValue o : list ) {
                            matches &= o.equals( ((List<?>) properties.get( entry.getKey() )).get( i ) );
                            i++;
                        }
                    } else if ( entry.getValue().isNumber() || other.properties.get( entry.getKey() ).isNumber() ) {
                        matches &=
                                toBigDecimal( other.properties.get( entry.getKey() ).toString() ).doubleValue()
                                        - toBigDecimal( entry.getValue().toString() ).doubleValue() < EPSILON;
                    } else {
                        matches &= other.properties.get( entry.getKey() ).equals( entry.getValue() );
                    }
                } else {
                    matches = false;
                }
            }
        }

        if ( labels != null ) {
            matches &= other.labels.containsAll( labels );
        }

        return matches;
    }

}
