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

import java.util.List;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.schema.graph.GraphObject;
import org.polypheny.db.schema.graph.GraphPropertyHolder;
import org.polypheny.db.schema.graph.PolyPath;

public class TestPath implements TestObject {

    private final List<TestGraphObject> objects;


    public TestPath( TestGraphObject[] objects ) {
        this.objects = List.of( objects );
    }


    public static TestPath of( TestGraphObject... objects ) {
        return new TestPath( objects );
    }


    @Override
    public boolean matches( Object other, boolean exclusive ) {
        assert other instanceof PolyPath;
        PolyPath path = (PolyPath) other;
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


    @Override
    public Object toPoly( String val ) {
        return CypherTestTemplate.GSON.fromJson( val, CypherTestTemplate.Type.from( this ).getPolyClass() );
    }

}
