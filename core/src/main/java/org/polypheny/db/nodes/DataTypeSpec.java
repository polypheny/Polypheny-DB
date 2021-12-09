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

package org.polypheny.db.nodes;

import java.util.TimeZone;
import org.polypheny.db.type.PolyType;

public interface DataTypeSpec extends Visitable {

    int getScale();

    int getPrecision();

    int getDimension();

    int getCardinality();

    String getCharSetName();

    TimeZone getTimeZone();

    Boolean getNullable();

    Identifier getCollectionsTypeName();

    PolyType getCollectionsType();

    PolyType getType();

}
