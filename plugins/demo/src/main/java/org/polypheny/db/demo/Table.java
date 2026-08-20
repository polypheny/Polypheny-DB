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

package org.polypheny.db.demo;

import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import java.util.ArrayList;
import java.util.List;

public class Table {
    private final String name;
    private final List<DdlManager.FieldInformation> columns;
    private final String file;

    public Table( String name, List<FieldInformation> columns, String file) {
        this.name = name;
        this.columns = columns;
        this.file = file;
    }

    public String getName() {
        return this.name;
    }

    public List<FieldInformation> getColumns() {
        return this.columns;
    }

    public String getFile() {
        return this.file;
    }

    public List<ConstraintInformation> getConstraints() {
        return new ArrayList<>();
    }
}
