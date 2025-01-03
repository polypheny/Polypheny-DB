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

package org.polypheny.db.transaction.locking;

import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyLong;

public class IdentifierUtils {

    public static final String IDENTIFIER_KEY = "_eid";
    public static final long MISSING_IDENTIFIER = 0;
    public static final AlgDataType IDENTIFIER_ALG_TYPE = ((PolyTypeFactoryImpl) AlgDataTypeFactoryImpl.DEFAULT).createBasicPolyType( PolyType.BIGINT, true );

    public static final ColumnTypeInformation IDENTIFIER_COLUMN_TYPE = new ColumnTypeInformation(
            PolyType.BIGINT, // binary not supported by hsqldb TODO TH: check for other stores, datatypes
            null,
            null,
            null,
            null,
            null,
            false
    );

    public static final FieldInformation IDENTIFIER_FIELD_INFORMATION = new FieldInformation(
            IDENTIFIER_KEY,
            IDENTIFIER_COLUMN_TYPE,
            Collation.CASE_INSENSITIVE,
            new PolyLong( MISSING_IDENTIFIER ),
            1
    );


    public static PolyString getIdentifierKeyAsPolyString() {
        return PolyString.of( IDENTIFIER_KEY );
    }


    public static void throwIllegalFieldName() {
        throw new IllegalArgumentException( MessageFormat.format(
                "The field {0} is reserved for internal use and cannot be used.",
                IdentifierUtils.IDENTIFIER_KEY )
        );
    }


    public static List<FieldInformation> addIdentifierFieldIfAbsent( List<FieldInformation> fields ) {
        if ( fields.get( 0 ).name().equals( IDENTIFIER_KEY ) ) {
            return fields;
        }
        List<FieldInformation> newFields = fields.stream()
                .map( f -> new FieldInformation( f.name(), f.typeInformation(), f.collation(), f.defaultValue(), f.position() + 1 ) )
                .collect( Collectors.toCollection( LinkedList::new ) );
        newFields.add( 0, IDENTIFIER_FIELD_INFORMATION );
        return newFields;
    }


    public static void throwIfIsIdentifierKey( String string ) {
        if ( IDENTIFIER_KEY.equals( string ) ) {
            throwIllegalFieldName();
        }
    }


    public static void throwIfContainsIdentifierKey( Set<String> fieldNames ) {
        if ( fieldNames.contains( IDENTIFIER_KEY ) ) {
            throwIllegalFieldName();
        }
    }


    public static void throwIfContainsIdentifierField( List<FieldInformation> fields ) {
        Set<String> fieldNames = fields.stream()
                .map( FieldInformation::name )
                .collect( Collectors.toSet() );
        throwIfContainsIdentifierKey( fieldNames );
    }

    public static boolean containsIdentifierKey( List<PolyDocument> documents ) {
        return documents.stream()
                .flatMap( v -> v.map.keySet().stream() )
                .map( PolyString::getValue )
                .anyMatch( value -> value.equals( IDENTIFIER_KEY ) );
    }

    public static void throwIfContainsIdentifierKey( LogicalLpgModify lpgModify ) {
        boolean modifiesIdentifier = lpgModify.getOperations().stream()
                .map(o -> o.unwrap( RexCall.class))
                .filter( Optional::isPresent)
                .flatMap(o -> o.get().getOperands().stream())
                .map(r -> r.unwrap( RexLiteral.class))
                .filter(Optional::isPresent)
                .map(v -> v.get().getValue())
                .filter( PolyValue::isString )
                .anyMatch(s -> IdentifierUtils.IDENTIFIER_KEY.equals(s.asString().getValue()));
        if (!modifiesIdentifier) {
            return;
        }
        throwIllegalFieldName();
    }


}
