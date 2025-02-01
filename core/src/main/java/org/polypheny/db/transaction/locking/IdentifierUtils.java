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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
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
    public static final String VERSION_KEY = "_vid";

    private static final Set<String> DISALLOWED_KEYS = Set.of(IDENTIFIER_KEY, VERSION_KEY);

    public static final long MISSING_IDENTIFIER = 0;
    public static final long MISSING_VERSION = 0;

    public static final AlgDataType IDENTIFIER_ALG_TYPE = ((PolyTypeFactoryImpl) AlgDataTypeFactoryImpl.DEFAULT).createBasicPolyType( PolyType.BIGINT, true );
    public static final AlgDataType VERSION_ALG_TYPE = ((PolyTypeFactoryImpl) AlgDataTypeFactoryImpl.DEFAULT).createBasicPolyType( PolyType.BIGINT, true );

    public static final ColumnTypeInformation IDENTIFIER_COLUMN_TYPE = new ColumnTypeInformation(
            PolyType.BIGINT, // binary not supported by hsqldb TODO TH: check for other stores, datatypes
            null,
            null,
            null,
            null,
            null,
            true
    );

    public static final FieldInformation IDENTIFIER_FIELD_INFORMATION = new FieldInformation(
            IDENTIFIER_KEY,
            IDENTIFIER_COLUMN_TYPE,
            Collation.CASE_INSENSITIVE,
            new PolyLong( MISSING_IDENTIFIER ),
            1
    );

    public static final ColumnTypeInformation VERSION_COLUMN_TYPE = new ColumnTypeInformation(
            PolyType.BIGINT,
            null,
            null,
            null,
            null,
            null,
            true
    );

    public static final FieldInformation VERSION_FIELD_INFORMATION = new FieldInformation(
            VERSION_KEY,
            VERSION_COLUMN_TYPE,
            Collation.CASE_INSENSITIVE,
            new PolyLong( MISSING_VERSION ),
            2
    );

    public static PolyString getIdentifierKeyAsPolyString() {
        return PolyString.of( IDENTIFIER_KEY );
    }

    public static PolyString getVersionKeyAsPolyString() {
        return PolyString.of( VERSION_KEY );
    }

    public static void throwIllegalFieldName() {
        throw new IllegalArgumentException( MessageFormat.format(
                "The fields {0} and {1} are reserved for internal use and cannot be used.",
                IDENTIFIER_KEY,
                VERSION_KEY)
        );
    }

    public static PolyLong getVersionAsPolyLong(long version, boolean isCommitted) {
        return PolyLong.of( isCommitted ? version : version * -1);
    }

    public static List<FieldInformation> addMvccFieldsIfAbsent(List<FieldInformation> fields) {
        List<FieldInformation> newFields = new LinkedList<>();

        boolean hasIdentifier = fields.get(0).name().equals(IDENTIFIER_KEY);
        boolean hasVersion = fields.size() > 1 && fields.get(1).name().equals(VERSION_KEY);

        if (!hasIdentifier) {
            newFields.add(IDENTIFIER_FIELD_INFORMATION);
        }

        if (!hasVersion) {
            newFields.add(VERSION_FIELD_INFORMATION);
        }

        fields.stream()
                .map(f -> new FieldInformation(
                        f.name(),
                        f.typeInformation(),
                        f.collation(),
                        f.defaultValue(),
                        f.position() + (hasIdentifier ? 0 : 1) + (hasVersion ? 0 : 1))
                )
                .forEach(newFields::add);

        return newFields;
    }



    public static void throwIfIsDisallowedKey(String string) {
        if (DISALLOWED_KEYS.contains(string)) {
            throwIllegalFieldName();
        }
    }


    public static void throwIfContainsDisallowedKey(Set<String> fieldNames) {
        if (!Collections.disjoint(fieldNames, DISALLOWED_KEYS)) {
            throwIllegalFieldName();
        }
    }


    public static void throwIfContainsDisallowedField(List<FieldInformation> fields) {
        Set<String> fieldNames = fields.stream()
                .map(FieldInformation::name)
                .collect(Collectors.toSet());
        throwIfContainsDisallowedKey(fieldNames);
    }

    public static boolean containsDisallowedKeys(List<PolyDocument> documents) {
        return documents.stream()
                .flatMap(v -> v.map.keySet().stream())
                .map(PolyString::getValue)
                .anyMatch(DISALLOWED_KEYS::contains);
    }


    public static void throwIfContainsDisallowedKey(LogicalLpgModify lpgModify) {
        boolean modifiesDisallowed = lpgModify.getOperations().stream()
                .map(o -> o.unwrap(RexCall.class))
                .filter(Optional::isPresent)
                .flatMap(o -> o.get().getOperands().stream())
                .map(r -> r.unwrap(RexLiteral.class))
                .filter(Optional::isPresent)
                .map(v -> v.get().getValue())
                .filter(PolyValue::isString)
                .map(s -> s.asString().getValue())
                .anyMatch(DISALLOWED_KEYS::contains);
        if (modifiesDisallowed) {
            throwIllegalFieldName();
        }
    }


}
