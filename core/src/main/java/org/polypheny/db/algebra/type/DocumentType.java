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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Data;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Collation;

@Data
public class DocumentType implements AlgDataType, AlgDataTypeFamily {

    public StructKind structKind;

    public List<AlgDataTypeField> fixedFields;


    public String physicalName = null;

    public String digest;


    public DocumentType( @Nonnull List<AlgDataTypeField> fixedFields ) {
        this.structKind = fixedFields.isEmpty() ? StructKind.NONE : StructKind.SEMI;
        this.fixedFields = new ArrayList<>( fixedFields );
        this.digest = computeDigest();
    }


    public DocumentType() {
        this( List.of( new AlgDataTypeFieldImpl( "_id_", 0, new DocumentType( List.of() ) ) ) );
    }


    public static AlgDataType asRelational() {
        return new AlgRecordType( List.of(
                new AlgDataTypeFieldImpl( "_id_", 1, AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.VARCHAR, 2024 ) ),
                new AlgDataTypeFieldImpl( "_data_", 1, AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.VARCHAR, 2024 ) )
        ) );
    }


    private String computeDigest() {
        assert fixedFields != null;
        return getClass().getSimpleName() +
                fixedFields.stream().map( f -> f.getType().getFullTypeString() ).collect( Collectors.joining( "$" ) );
    }


    @Override
    public boolean isStruct() {
        return true;
    }


    @Override
    public List<AlgDataTypeField> getFieldList() {
        return fixedFields;
    }


    @Override
    public List<String> getFieldNames() {
        return getFieldList().stream().map( AlgDataTypeField::getName ).collect( Collectors.toList() );
    }


    @Override
    public int getFieldCount() {
        return getFieldList().size();
    }


    @Override
    public AlgDataTypeField getField( String fieldName, boolean caseSensitive, boolean elideRecord ) {
        // everything we ask a document for is there
        int index = getFieldNames().indexOf( fieldName );
        if ( index >= 0 ) {
            return getFieldList().get( index );
        }
        AlgDataTypeFieldImpl added = new AlgDataTypeFieldImpl( fieldName, getFieldCount(), new DocumentType() );
        fixedFields.add( added );
        computeDigest();

        return added;
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


}
