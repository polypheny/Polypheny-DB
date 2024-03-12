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


import java.util.List;
import lombok.SneakyThrows;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.GraphObject;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyPath;

public class TestPath implements TestObject {

    private final List<TestGraphObject> objects;


    public TestPath( TestGraphObject[] objects ) {
        this.objects = List.of( objects );
    }


    public static TestPath of( TestGraphObject... objects ) {
        return new TestPath( objects );
    }


    @Override
    public boolean matches( PolyValue other, boolean exclusive ) {
        assert other.isPath();
        PolyPath path = other.asPath();
        List<GraphPropertyHolder> elements = path.getPath();

        if ( elements.size() != objects.size() ) {
            return false;
        }

        int i = 0;
        boolean matches = true;
        for ( GraphObject element : elements ) {
            matches &= objects.get( i ).matches( element, exclusive );
            i++;
        }

        return matches;
    }


    @SneakyThrows
    @Override
    public PolyValue toPoly( String val ) {
        return val == null ? null : PolyValue.fromTypedJson( val, CypherTestTemplate.Type.from( this ).getPolyClass() );
    }

}
