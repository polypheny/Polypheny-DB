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

package org.polypheny.db.transaction.mvcc;

import java.text.MessageFormat;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;

public class IdentifierUtils {

    public static final String IDENTIFIER_KEY = "_eid";
    public static final String VERSION_KEY = "_vid";

    public static final long MISSING_IDENTIFIER = 0;

    public static final AlgDataType IDENTIFIER_ALG_TYPE = ((PolyTypeFactoryImpl) AlgDataTypeFactoryImpl.DEFAULT).createBasicPolyType( PolyType.DECIMAL, false );
    public static final AlgDataType VERSION_ALG_TYPE = ((PolyTypeFactoryImpl) AlgDataTypeFactoryImpl.DEFAULT).createBasicPolyType( PolyType.DECIMAL, false );

    public static final ColumnTypeInformation IDENTIFIER_COLUMN_TYPE = new ColumnTypeInformation(
            IDENTIFIER_ALG_TYPE.getPolyType(), // binary not supported by hsqldb TODO TH: check for other stores, datatypes
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
            null,
            1,
            true
    );

    public static final ColumnTypeInformation VERSION_COLUMN_TYPE = new ColumnTypeInformation(
            VERSION_ALG_TYPE.getPolyType(),
            null,
            null,
            null,
            null,
            null,
            false
    );

    public static final FieldInformation VERSION_FIELD_INFORMATION = new FieldInformation(
            VERSION_KEY,
            VERSION_COLUMN_TYPE,
            Collation.CASE_INSENSITIVE,
            null,
            2,
            true
    );


    public static PolyString getIdentifierKeyAsPolyString() {
        return PolyString.of( IDENTIFIER_KEY );
    }


    public static PolyString getVersionKeyAsPolyString() {
        return PolyString.of( VERSION_KEY );
    }


    public static void throwStartsWithUnderscore( String fieldName ) {
        throw new IllegalArgumentException( MessageFormat.format(
                "The name of the field {0} starts with an '_' which is illegal. Names with leading '_' are reserved for internal use.",
                fieldName )
        );
    }


    public static void throwStartsWithNumber( String fieldName ) {
        throw new IllegalArgumentException( MessageFormat.format(
                "The name of the field {0} starts with an digit which is illegal.",
                fieldName )
        );
    }

    public static boolean isIdentifier( AlgDataTypeField field ) {
        if ( field.getName().equals( IDENTIFIER_KEY ) ) {
            return true;
        }
        return field.getName().equals( VERSION_KEY );
    }

    public static boolean isIdentifier( AlgDataTypeField field ) {
        return isIdentifier( field.getName() );
    }


    public static boolean isIdentifier( String fieldName ) {
        if ( fieldName.equals( IDENTIFIER_KEY ) ) {
            return true;
        }
        return fieldName.equals( VERSION_KEY );
    }


    public static AlgDataType removeIdentifierFields( AlgDataType type, AlgDataTypeFactory typeFactory ) {
        return typeFactory.createStructType( type.getFields().stream().filter( f -> !isIdentifier( f ) ).toList() );
    }


    public static PolyBigDecimal getVersionAsPolyBigDecimal( long version, boolean isCommitted ) {
        return PolyBigDecimal.of( isCommitted ? version : version * -1 );
    }


    public static List<FieldInformation> addIdentifierFieldsIfAbsent( List<FieldInformation> fields ) {
        List<FieldInformation> newFields = new LinkedList<>();

        boolean hasIdentifier = fields.get( 0 ).name().equals( IDENTIFIER_KEY );
        boolean hasVersion = fields.size() > 1 && fields.get( 1 ).name().equals( VERSION_KEY );

        if ( !hasIdentifier ) {
            newFields.add( IDENTIFIER_FIELD_INFORMATION );
        }

        if ( !hasVersion ) {
            newFields.add( VERSION_FIELD_INFORMATION );
        }

        fields.stream()
                .map( f -> new FieldInformation(
                        f.name(),
                        f.typeInformation(),
                        f.collation(),
                        f.defaultValue(),
                        f.position() + (hasIdentifier ? 0 : 1) + (hasVersion ? 0 : 1),
                        false )
                )
                .forEach( newFields::add );

        return newFields;
    }

    public static void throwIfIsDisallowedFieldName( String fieldName ) {
        throwIfIsDisallowedFieldName( fieldName, false );
    }


    public static void throwIfIsDisallowedFieldName( String fieldName, boolean ignoreDocId ) {
        if ( ignoreDocId && fieldName.matches( "_id" ) ) {
            return;
        }
        if ( fieldName.startsWith( "_" ) ) {
            throwStartsWithUnderscore( fieldName );
        }
        if ( fieldName.matches( "^[^A-Za-z].*" ) ) {
            throwStartsWithNumber( fieldName );
        }
    }


    public static void throwIfContainsDisallowedFieldName( Set<String> fieldNames ) {
        fieldNames.forEach( IdentifierUtils::throwIfIsDisallowedFieldName );
    }


    public static void throwIfContainsDisallowedField( List<FieldInformation> fields ) {
        Set<String> fieldNames = fields.stream()
                .map( FieldInformation::name )
                .collect( Collectors.toSet() );
        throwIfContainsDisallowedFieldName( fieldNames );
    }


    public static void throwIfContainsDisallowedFieldName( List<PolyDocument> documents ) {
        documents.stream()
                .flatMap( v -> v.map.keySet().stream() )
                .map( PolyString::getValue )
                .forEach( f -> IdentifierUtils.throwIfIsDisallowedFieldName( f, true ) );
    }


    public static void throwIfContainsDisallowedFieldName( PolyDictionary dictionary ) {
        dictionary.keySet().stream()
                .map( PolyString::getValue )
                .forEach( IdentifierUtils::throwIfIsDisallowedFieldName );
    }


    public static void throwIfContainsDisallowedFieldName( LogicalLpgModify lpgModify ) {
        lpgModify.getOperations().stream()
                .map( o -> o.unwrap( RexCall.class ) )
                .filter( Optional::isPresent )
                .flatMap( o -> o.get().getOperands().stream() )
                .map( r -> r.unwrap( RexLiteral.class ) )
                .filter( Optional::isPresent )
                .map( v -> v.get().getValue() )
                .filter( PolyValue::isString )
                .map( s -> s.asString().getValue() )
                .forEach( IdentifierUtils::throwIfIsDisallowedFieldName );
    }
}
