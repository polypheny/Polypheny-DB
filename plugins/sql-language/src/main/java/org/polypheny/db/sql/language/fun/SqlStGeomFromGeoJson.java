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

package org.polypheny.db.sql.language.fun;

import static org.polypheny.db.sql.language.fun.SqlStGeomFromText.ST_GEOMFROMTEXT_ARG_CHECKER;

import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.type.inference.ReturnTypes;

/**
 * Definition of the "ST_GeomFromGeoJson" spatial function.
 * The function has a required parameter - TWKB binary representation
 * and an optional SRID integer.
 */
public class SqlStGeomFromGeoJson extends SqlFunction {

    /**
     * Creates the ST_GeomFromGeoJson.
     */
    public SqlStGeomFromGeoJson() {
        super( "ST_GEOMFROMGEOJSON", Kind.GEO, ReturnTypes.GEOMETRY, null, ST_GEOMFROMTEXT_ARG_CHECKER, FunctionCategory.GEOMETRY );
    }


    @Override
    public String getSignatureTemplate( int operandsCount ) {
        return switch ( operandsCount ) {
            case 1 -> "{0}({1})";
            case 2 -> "{0}({1}, {2})";
            default -> throw new AssertionError();
        };
    }

}
