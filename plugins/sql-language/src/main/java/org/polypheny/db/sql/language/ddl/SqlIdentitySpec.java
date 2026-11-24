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

package org.polypheny.db.sql.language.ddl;

import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.IdentityColumn;
import org.polypheny.db.catalog.entity.logical.IdentityColumn.Mode;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.PolyType;

/** A column's identity declaration, before the DDL creates the sequence. */
public record SqlIdentitySpec(Mode mode, Options options) {

    public static final SqlIdentitySpec SERIAL = new SqlIdentitySpec( Mode.BY_DEFAULT, Options.EMPTY );

    public IdentityColumn create( ColumnTypeInformation type ) {
        if ( type.collectionType() != null ) {
            throw new GenericRuntimeException( "An identity column requires a scalar integer type." );
        }
        return IdentityColumn.create( type.type(), mode, options.start(), options.increment(), options.min(), options.max(), Boolean.TRUE.equals( options.cycle() ) );
    }


    public void unparse( SqlWriter writer ) {
        writer.keyword( "GENERATED" );
        unparseMode( writer, mode );
        writer.keyword( "AS IDENTITY" );
        options.unparseGroup( writer );
    }


    public static void unparseMode( SqlWriter writer, Mode mode ) {
        writer.keyword( mode == Mode.ALWAYS ? "ALWAYS" : "BY DEFAULT" );
    }


    public record Options(Long start, Long increment, Long min, Long max, Boolean cycle, boolean noMin, boolean noMax) {

        public static final Options EMPTY = new Options( null, null, null, null, null, false, false );


        public IdentityColumn apply( PolyType type, IdentityColumn existing ) {
            long step = increment == null ? existing.increment() : increment;
            Long minimum = noMin ? Long.valueOf( IdentityColumn.defaultMin( type, step ) ) : min;
            Long maximum = noMax ? Long.valueOf( IdentityColumn.defaultMax( type, step ) ) : max;
            return Catalog.getInstance().getIdentitySequences().reconfigure( type, existing, start, increment, minimum, maximum, cycle );
        }


        public void unparseGroup( SqlWriter writer ) {
            if ( equals( EMPTY ) ) {
                return;
            }
            SqlWriter.Frame frame = writer.startList( "(", ")" );
            unparse( writer );
            writer.endList( frame );
        }


        public void unparse( SqlWriter writer ) {
            if ( start != null ) {
                writer.keyword( "START WITH" );
                writer.literal( start.toString() );
            }
            if ( increment != null ) {
                writer.keyword( "INCREMENT BY" );
                writer.literal( increment.toString() );
            }
            if ( min != null ) {
                writer.keyword( "MINVALUE" );
                writer.literal( min.toString() );
            } else if ( noMin ) {
                writer.keyword( "NO MINVALUE" );
            }
            if ( max != null ) {
                writer.keyword( "MAXVALUE" );
                writer.literal( max.toString() );
            } else if ( noMax ) {
                writer.keyword( "NO MAXVALUE" );
            }
            if ( cycle != null ) {
                writer.keyword( cycle ? "CYCLE" : "NO CYCLE" );
            }
        }

    }

}
