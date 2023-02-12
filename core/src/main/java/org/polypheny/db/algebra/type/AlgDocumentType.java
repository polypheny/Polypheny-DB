/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.algebra.type;

import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Collation;

public class AlgDocumentType implements Serializable, AlgDataType, AlgDataTypeFamily, AlgDataTypeField {

    @Getter
    private final StructKind structKind;
    private final ImmutableList<AlgDataType> fixedFields;

    @Getter
    private final String name;

    @Getter
    @Setter
    private String physicalName = null;


    public AlgDocumentType( @Nullable String name, @Nonnull List<AlgDataType> fixedFields ) {
        this.name = name;
        this.structKind = fixedFields.isEmpty() ? StructKind.NONE : StructKind.SEMI;
        assert fixedFields != null;
        this.fixedFields = ImmutableList.copyOf( fixedFields );
    }


    public AlgDocumentType() {
        this( null, List.of( new AlgDocumentType( "_id_", List.of() ) ) );
    }


    public AlgDataType asRelational() {
        return new AlgRecordType( List.of( new AlgDataTypeFieldImpl( "_data_", 1, AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.VARCHAR, 2024 ) ) ) );
    }


    @Override
    public boolean isStruct() {
        return false;
    }


    @Override
    public List<AlgDataTypeField> getFieldList() {
        return List.of( this );
    }


    @Override
    public List<String> getFieldNames() {
        if ( name == null ) {
            return List.of( "$d" );
        }
        return List.of( name );
    }


    @Override
    public int getFieldCount() {
        return 1;
    }


    @Override
    public AlgDataTypeField getField( String fieldName, boolean caseSensitive, boolean elideRecord ) {
        throw new RuntimeException( "getField on DocumentType" );

    }


    @Override
    public boolean isNullable() {
        return false;
    }


    @Override
    public AlgDataType getComponentType() {
        return null;
    }


    @Override
    public AlgDataType getKeyType() {
        return null;
    }


    @Override
    public AlgDataType getValueType() {
        return null;
    }


    @Override
    public Charset getCharset() {
        return null;
    }


    @Override
    public Collation getCollation() {
        return null;
    }


    @Override
    public IntervalQualifier getIntervalQualifier() {
        return null;
    }


    @Override
    public int getPrecision() {
        return 0;
    }


    @Override
    public int getRawPrecision() {
        return 0;
    }


    @Override
    public int getScale() {
        return 0;
    }


    @Override
    public PolyType getPolyType() {
        return null;
    }


    @Override
    public String getFullTypeString() {
        return null;
    }


    @Override
    public AlgDataTypeFamily getFamily() {
        return null;
    }


    @Override
    public AlgDataTypePrecedenceList getPrecedenceList() {
        return null;
    }


    @Override
    public AlgDataTypeComparability getComparability() {
        return AlgDataTypeComparability.ALL;
    }


    @Override
    public boolean isDynamicStruct() {
        return false;
    }


    @Override
    public int getIndex() {
        return 0;
    }


    @Override
    public AlgDataType getType() {
        return this;
    }


    @Override
    public boolean isDynamicStar() {
        return false;
    }


    @Override
    public String getKey() {
        if ( name == null ) {
            return "$d";
        }
        return name;
    }


    @Override
    public AlgDataType getValue() {
        return this;
    }


    @Override
    public AlgDataType setValue( AlgDataType value ) {
        throw new RuntimeException( "Error while setting field on AlgDocumentType" );
    }

}
