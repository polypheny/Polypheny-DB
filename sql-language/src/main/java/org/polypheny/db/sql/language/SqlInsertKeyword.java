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

package org.polypheny.db.sql.language;


import org.polypheny.db.languages.ParserPos;


/**
 * Defines the keywords that can occur immediately after the "INSERT" keyword.
 *
 * Standard SQL has no such keywords, but extension projects may define them.
 */
public enum SqlInsertKeyword {
    UPSERT;


    /**
     * Creates a parse-tree node representing an occurrence of this keyword at a particular position in the parsed text.
     */
    public SqlLiteral symbol( ParserPos pos ) {
        return SqlLiteral.createSymbol( this, pos );
    }
}

