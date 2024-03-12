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

package org.polypheny.db.sql.language;


import java.util.List;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;


/**
 * Abstract base for character and binary string literals.
 */
public abstract class SqlAbstractStringLiteral extends SqlLiteral {

    protected SqlAbstractStringLiteral( PolyString value, PolyType typeName, ParserPos pos ) {
        super( value, typeName, pos );
    }


    /**
     * Helper routine for {@link SqlUtil#concatenateLiterals}.
     *
     * @param literals homogeneous StringLiteral args
     * @return StringLiteral with concatenated value. this == lits[0], used only for method dispatch.
     */
    protected abstract SqlAbstractStringLiteral concat1( List<SqlLiteral> literals );

}

