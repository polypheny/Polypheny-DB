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

package org.polypheny.db.transaction.locking;

import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.polypheny.db.type.PolyType;

public class EntityIdentifierUtils {

    public static final String IDENTIFIER_KEY = "_eid";

    public static final ColumnTypeInformation IDENTIFIER_COLUMN_TYPE = new ColumnTypeInformation(
            PolyType.BIGINT, // binary not supported by hsqldb
            null,
            null,
            null,
            null,
            null,
            false
    );

    public static final FieldInformation IDENTIFIER_FIELD_INFORMATION = new FieldInformation(
            IDENTIFIER_KEY,
            IDENTIFIER_COLUMN_TYPE,
            Collation.CASE_INSENSITIVE,
            null,
            1
    );

}
