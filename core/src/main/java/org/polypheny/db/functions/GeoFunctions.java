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

package org.polypheny.db.functions;

import static org.polypheny.db.functions.Functions.toUnchecked;

import org.polypheny.db.type.entity.PolyFloat;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.spatial.InvalidGeometryException;
import org.polypheny.db.type.entity.spatial.PolyGeometry;

public class GeoFunctions {

    private GeoFunctions() {
        // empty on purpose
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyGeometry stGeoFromText( PolyString wkt ) {
        try {
            return PolyGeometry.of( wkt.value );
        }
        catch ( InvalidGeometryException e ) {
            throw toUnchecked( e );
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    public static PolyFloat stX( PolyGeometry geometry ) {
        if (!geometry.isPoint()) {
            throw toUnchecked( new InvalidGeometryException( "This function could be applied only to points" ));
        }
        return PolyFloat.of( geometry.asPoint().getX() );
    }

    public static PolyString test1( PolyString wkt ) {
        return wkt;
    }

}
