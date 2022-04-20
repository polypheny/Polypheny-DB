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

package org.polypheny.db.adaptimizer.randomtrees;

import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

/**
 * Interface for options in random tree generation.
 */
public interface RandomTreeTemplate {

    /**
     * Provides a random operator type.
     */
    String nextOperator();

    /**
     * Provides a random table.
     */
    Pair<String, TableRecord> nextTable();

    /**
     * Provides a random column for a table.
     */
    Pair<String, PolyType> nextColumn( TableRecord adaptiveTableRecord );

    Pair<String, String> nextPolyTypePartners( TableRecord adaptiveTableRecord );

    Pair<String, String> nextPolyTypePartners( TableRecord adaptiveTableRecordA, TableRecord adaptiveTableRecordB );

    Pair<String, String> nextJoinColumnsWithForeignKeys( TableRecord adaptiveTableRecordA, TableRecord adaptiveTableRecordB );

    Pair<String, String> nextJoinColumns( TableRecord adaptiveTableRecordA, TableRecord adaptiveTableRecordB );

}
