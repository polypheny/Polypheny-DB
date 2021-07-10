/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.document;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.types.ObjectId;
import org.polypheny.db.jdbc.JavaTypeFactoryImpl;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rel.type.DynamicRecordTypeImpl;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.sql.SqlCollation;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.util.NlsString;

public class DocumentValidator {

    private final static PolyTypeFactoryImpl typeFactory = new PolyTypeFactoryImpl( RelDataTypeSystem.DEFAULT );
    private final static Gson gson = new Gson();


    public static Values validateValues( Values values ) {
        RelDataType rowType = new DynamicRecordTypeImpl( new JavaTypeFactoryImpl() );
        List<ImmutableList<RexLiteral>> parsed = new ArrayList<>();
        List<String> names = values.getRowType().getFieldNames();

        if ( !names.contains( "_id" ) || !names.contains( "_data" ) ) {
            rowType.getField( "_id", false, false );
            rowType.getField( "_data", false, false );
            for ( ImmutableList<RexLiteral> tuple : values.tuples ) {
                parsed.add( ImmutableList.copyOf( validateRow( tuple, values.getRowType(), rowType ) ) );
            }

            return LogicalValues.create( values.getCluster(), rowType, ImmutableList.copyOf( parsed ) );
        } else {
            return values;
        }

    }


    private static List<RexLiteral> validateRow( ImmutableList<RexLiteral> tuple, RelDataType oldRowType, RelDataType newRowType ) {
        List<RexLiteral> values = new ArrayList<>();
        Map<String, Object> data = new HashMap<>();
        RexLiteral id = null;
        int pos = 0;
        for ( RelDataTypeField field : oldRowType.getFieldList() ) {
            if ( field.getName().equals( "_id" ) ) {
                id = tuple.get( pos );
            } else if ( field.getName().equals( "_data" ) ) {
                data.put( "_data", tuple.get( pos ) );
            } else {
                data.put( field.getName(), tuple.get( pos ) );
            }
            pos++;
        }
        if ( id == null ) {
            id = new RexLiteral( new NlsString( ObjectId.get().toString(), "ISO-8859-1", SqlCollation.IMPLICIT ), typeFactory.createPolyType( PolyType.CHAR, 24 ), PolyType.CHAR );
        }
        values.add( id );

        String parsed = gson.toJson( data );
        RexLiteral literal = new RexLiteral( new NlsString( parsed, "ISO-8859-1", SqlCollation.IMPLICIT ), typeFactory.createPolyType( PolyType.JSON ), PolyType.JSON );
        values.add( literal );

        return values;
    }


}
