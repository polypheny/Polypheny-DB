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


import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.util.BitString;


/**
 * A binary (or hexadecimal) string literal.
 * <p>
 * The {@link #value} field is a {@link BitString} and {@code #typeName} is {@link PolyType#BINARY}.
 */
public class SqlBinaryStringLiteral extends SqlLiteral {


    protected SqlBinaryStringLiteral( PolyBinary val, ParserPos pos ) {
        super( val, PolyType.BINARY, pos );
    }


    @Override
    public SqlBinaryStringLiteral clone( ParserPos pos ) {
        return new SqlBinaryStringLiteral( (PolyBinary) value, pos );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        assert value.isBinary();
        writer.literal( "X'" + value.asBinary().toHexString() + "'" );
    }


}

