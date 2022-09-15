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

package org.polypheny.db.cypher.helper;

import static org.junit.Assert.fail;
import static org.polypheny.db.runtime.functions.Functions.toBigDecimal;

import com.google.gson.Gson;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.schema.graph.GraphPropertyHolder;
import org.polypheny.db.util.Pair;

public class TestGraphObject implements TestObject {

    public static double EPSILON = 0.2;

    @Nullable
    final String id;

    @Nullable
    final Map<String, Object> properties;

    @Nullable
    final List<String> labels;


    public TestGraphObject( @Nullable String id, @Nullable Map<String, Object> properties, @Nullable List<String> labels ) {
        this.id = id;
        this.properties = properties;
        this.labels = labels;
    }


    @NotNull
    public static Map<String, Object> getProps( Pair<String, Object>[] properties ) {
        Map<String, Object> props = new HashMap<>();

        for ( Pair<String, Object> property : properties ) {
            props.put( property.left, property.right );
        }
        return props;
    }


    @Override
    public boolean matches( Object other, boolean exclusive ) {
        assert other instanceof GraphPropertyHolder;
        return matches( (GraphPropertyHolder) other, exclusive );
    }


    @Override
    public Object toPoly( String val ) {
        return CypherTestTemplate.GSON.fromJson( val, CypherTestTemplate.Type.from( this ).getPolyClass() );
    }


    public boolean matches( GraphPropertyHolder other, boolean exclusive ) {
        boolean matches = true;
        if ( id != null ) {
            matches = id.equals( other.id );
        }
        if ( properties != null ) {
            if ( exclusive ) {
                matches &= properties.size() == other.properties.size();
            }
            for ( Entry<String, Object> entry : properties.entrySet() ) {
                if ( other.properties.containsKey( entry.getKey() ) ) {
                    if ( entry.getValue() instanceof List ) {
                        int i = 0;
                        List<?> list;
                        Object property = other.properties.get( entry.getKey() );

                        if ( property instanceof List ) {
                            list = (List<?>) property;
                        } else if ( property instanceof String ) {
                            list = new Gson().fromJson( (String) other.properties.get( entry.getKey() ), List.class );
                        } else {
                            fail( "comparison with list is not possible" );
                            throw new RuntimeException();
                        }

                        for ( Object o : list ) {
                            matches &= o.equals( ((List<?>) properties.get( entry.getKey() )).get( i ) );
                            i++;
                        }
                    } else if ( entry.getValue() instanceof Number || other.properties.get( entry.getKey() ) instanceof Number ) {
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
            matches &= labels.containsAll( other.labels );
        }

        return matches;
    }

}
