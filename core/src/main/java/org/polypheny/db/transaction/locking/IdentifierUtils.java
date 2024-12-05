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

import com.google.common.collect.ImmutableList;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyLong;

public class IdentifierUtils {

    public static final String IDENTIFIER_KEY = "_eid";
    public static final long MISSING_IDENTIFIER = 0;

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

    public static PolyLong getIdentifier() {
        return new PolyLong( IdentifierRegistry.INSTANCE.getEntryIdentifier() );
    }

    public static void throwIllegalFieldName() {
        throw new IllegalArgumentException( MessageFormat.format(
                "The field {0} is reserved for internal use and cannot be used.",
                IdentifierUtils.IDENTIFIER_KEY)
        );
    }

    public static AlgNode overwriteIdentifierInInput( LogicalRelValues value ) {
        List<List<RexLiteral>> newValues = new ArrayList<>();
        value.tuples.forEach(row -> {
            List<RexLiteral> newRow = new ArrayList<>(row);
            RexLiteral identifierLiteral = newRow.get(0);
            newRow.set(0, IdentifierUtils.copyAndUpdateIdentifier(identifierLiteral, getIdentifier()));
            newValues.add(newRow);
        });

        ImmutableList<ImmutableList<RexLiteral>> immutableValues = new ImmutableList.Builder<ImmutableList<RexLiteral>>()
                .addAll(newValues.stream()
                        .map(ImmutableList::copyOf)
                        .toList())
                .build();

        return new LogicalRelValues(
                value.getCluster(),
                value.getTraitSet(),
                value.getRowType(),
                immutableValues
        );
    }

    private static RexLiteral copyAndUpdateIdentifier( RexLiteral identifierLiteral, PolyLong identifier ) {
        return new RexLiteral(identifier,  identifierLiteral.getType(), PolyType.DECIMAL);
    }


    public static List<FieldInformation> addIdentifierFieldIfAbsent( List<FieldInformation> fields ) {
        if (fields.get(0).name().equals( IDENTIFIER_KEY )){
            return fields;
        }
        List<FieldInformation> newFields = fields.stream()
                .map( f -> new FieldInformation(f.name(), f.typeInformation(), f.collation(), f.defaultValue(), f.position() + 1) )
                .collect( Collectors.toCollection( LinkedList::new ) );
        newFields.add(0, IDENTIFIER_FIELD_INFORMATION );
        return newFields;
    }

    public static void throwIfContainsIdentifierField(List<FieldInformation> fields) {
        if (fields.stream().noneMatch( f -> f.name().equals(IDENTIFIER_FIELD_INFORMATION.name()))) {
            return;

        }
        throwIllegalFieldName();
    }

}
