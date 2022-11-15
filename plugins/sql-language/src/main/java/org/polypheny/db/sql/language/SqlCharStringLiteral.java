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


import java.util.List;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Bug;
import org.polypheny.db.util.Collation;
import org.polypheny.db.util.NlsString;
import org.polypheny.db.util.Util;


/**
 * A character string literal.
 * <p>
 * Its {@link #value} field is an {@link NlsString} and {@code #typeName} is {@link PolyType#CHAR}.
 */
public class SqlCharStringLiteral extends SqlAbstractStringLiteral {

    protected SqlCharStringLiteral( NlsString val, ParserPos pos ) {
        super( val, PolyType.CHAR, pos );
    }


    /**
     * @return the underlying NlsString
     */
    public NlsString getNlsString() {
        return (NlsString) value;
    }


    /**
     * @return the collation
     */
    public Collation getCollation() {
        return getNlsString().getCollation();
    }


    @Override
    public SqlCharStringLiteral clone( ParserPos pos ) {
        return new SqlCharStringLiteral( (NlsString) value, pos );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        if ( false ) {
            Util.discard( Bug.FRG78_FIXED );
            String stringValue = ((NlsString) value).getValue();
            writer.literal( writer.getDialect().quoteStringLiteral( stringValue ) );
        }
        assert value instanceof NlsString;
        writer.literal( value.toString() );
    }


    @Override
    protected SqlAbstractStringLiteral concat1( List<SqlLiteral> literals ) {
        return new SqlCharStringLiteral(
                NlsString.concat(
                        Util.transform(
                                literals,
                                literal -> ((SqlCharStringLiteral) literal).getNlsString() ) ),
                literals.get( 0 ).getPos() );
    }

}

