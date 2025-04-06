/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.adapter.oracle;


import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.AlgDataTypeSystemImpl;
import org.polypheny.db.sql.language.SqlDialect;
import org.polypheny.db.type.PolyType;
import java.util.Objects;

public class OracleSqlDialect extends SqlDialect {


    /*
    TODO: Find out if this SqlDialect is really necessary (PostgreSql does have it, MySql doesn't).
     */
    private static final AlgDataTypeSystem ORACLE_TYPE_SYSTEM =
            new AlgDataTypeSystemImpl() {
                @Override
                public int getMaxPrecision( PolyType typeName ) {
                    if ( Objects.requireNonNull( typeName ) == PolyType.VARCHAR ) {// From htup_details.h in postgresql:
                        // MaxAttrSize is a somewhat arbitrary upper limit on the declared size of data fields of char(n) and similar types.  It need not have anything
                        // directly to do with the *actual* upper limit of varlena values, which is currently 1Gb (see TOAST structures in postgres.h).  I've set it
                        // at 10Mb which seems like a reasonable number --- tgl 8/6/00.
                        return 10 * 1024 * 1024;
                    }
                    return super.getMaxPrecision( typeName );
                }
            };


    public static final SqlDialect DEFAULT =
            new OracleSqlDialect( EMPTY_CONTEXT
                    .withNullCollation( NullCollation.HIGH )
                    .withIdentifierQuoteString( "\"" )
                    .withDataTypeSystem( ORACLE_TYPE_SYSTEM ) );



    public OracleSqlDialect( Context context ) { super( context ); }

}
