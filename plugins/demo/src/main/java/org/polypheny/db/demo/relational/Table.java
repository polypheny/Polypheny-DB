/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.demo.relational;

import org.checkerframework.checker.units.qual.A;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public record Table (String name, List<FieldInformation> columns, List<ConstraintInformation> constraints, String file ) {
    public String getPreparedStatementInsertQuery() {
        int length = this.columns.size();
        String params = "?, ".repeat( length );
        params = params.substring( 0, params.length() - 2 );
        return String.format("INSERT INTO %s VALUES (%s)", this.name, params);
    }
}
