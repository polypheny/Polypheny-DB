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

import java.util.List;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.sql.language.SqlFunction;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.InferTypes;
import org.polypheny.db.type.inference.ReturnTypes;

/**
 * Definition of the "ST_GeoFromText" spatial function.
 */
public class SqlStGeoFromText extends SqlFunction {

    /**
     * Creates the SqlStGeoFromText.
     */
    public SqlStGeoFromText() {
        super(
                "ST_GEOFROMTEXT",
                Kind.OTHER_FUNCTION,
                ReturnTypes.GEOMETRY,
                null,
                OperandTypes.STRING,
                FunctionCategory.GEOMETRY );
    }

    // TODO: add varying arguments for SRID

}
