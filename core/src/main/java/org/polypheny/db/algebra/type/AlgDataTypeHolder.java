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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.algebra.type;


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Holding the expandable list of fields for dynamic table.
 */
class AlgDataTypeHolder {

    private final List<AlgDataTypeField> fields = new ArrayList<>();
    private final AlgDataTypeFactory typeFactory;


    AlgDataTypeHolder( AlgDataTypeFactory typeFactory ) {
        this.typeFactory = typeFactory;
    }


    public List<AlgDataTypeField> getFieldList() {
        return fields;
    }


    public int getFieldCount() {
        return fields.size();
    }


    /**
     * Get field if exists, otherwise inserts a new field. The new field by default will have "any" type, except for the
     * dynamic star field.
     *
     * @param fieldName Request field name
     * @param caseSensitive Case Sensitive
     * @return A pair of RelDataTypeField and Boolean. Boolean indicates whether a new field is added to this holder.
     */
    Pair<AlgDataTypeField, Boolean> getFieldOrInsert( String fieldName, boolean caseSensitive ) {
        // First check if this field name exists in our field list
        for ( AlgDataTypeField f : fields ) {
            if ( Util.matches( caseSensitive, f.getName(), fieldName ) ) {
                return Pair.of( f, false );
            }
            // A dynamic star field matches any field
            if ( f.getType().getPolyType() == PolyType.DYNAMIC_STAR ) {
                return Pair.of( f, false );
            }
        }

        final PolyType typeName = DynamicRecordType.isDynamicStarColName( fieldName )
                ? PolyType.DYNAMIC_STAR
                : PolyType.ANY;

        // This field does not exist in our field list; add it
        AlgDataTypeField newField = new AlgDataTypeFieldImpl(
                -1L,
                fieldName,
                fields.size(),
                typeFactory.createTypeWithNullability( typeFactory.createPolyType( typeName ), true ) );

        // Add the name to our list of field names
        fields.add( newField );

        return Pair.of( newField, true );
    }


    public List<String> getFieldNames() {
        return fields.stream().map( AlgDataTypeField::getName ).collect( Collectors.toList() );
    }

}

