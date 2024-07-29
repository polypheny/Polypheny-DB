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

package org.polypheny.db.algebra.type;

import com.google.common.collect.Streams;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Data;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Collation;

@Data
public class DocumentType implements AlgDataType, AlgDataTypeFamily {

    public static final String DOCUMENT_ID = "_id";
    public static final String DOCUMENT_DATA = "_data";

    public StructKind structKind;

    public final List<AlgDataTypeField> fixed;

    public final List<String> excluded;

    boolean isFixed = false;

    public String physicalName = null;

    public String digest;


    public DocumentType( @NotNull List<AlgDataTypeField> fixed, @NotNull List<String> excluded ) {
        this.structKind = fixed.isEmpty() ? StructKind.NONE : StructKind.SEMI;
        this.excluded = excluded;
        this.fixed = new ArrayList<>( fixed );
        this.digest = computeDigest();
    }


    public DocumentType( @Nonnull List<AlgDataTypeField> fixed ) {
        this( fixed, List.of() );
    }


    public DocumentType() {
        this( List.of() );
    }


    public static DocumentType ofId() {
        return new DocumentType( List.of( new AlgDataTypeFieldImpl( -1L, DOCUMENT_ID, 0, new DocumentType( List.of() ) ) ) );
    }


    public static AlgDataType ofRelational() {
        return new AlgRecordType( List.of(
                getRelationalId(),
                getRelationalData()
        ) );
    }


    @NotNull
    private static AlgDataTypeFieldImpl getRelationalData() {
        return new AlgDataTypeFieldImpl( -1L, DOCUMENT_DATA, 1, AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.TEXT ) );
    }


    @NotNull
    public static AlgDataTypeFieldImpl getRelationalId() {
        return new AlgDataTypeFieldImpl( -1L, DOCUMENT_ID, 0, AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.TEXT ) );
    }


    public static AlgDataType ofDoc() {
        return new DocumentType( List.of( new AlgDataTypeFieldImpl( -1L, "d", 0, DocumentType.ofId() ) ) );
    }


    public static DocumentType ofIncludes( Map<String, ? extends RexNode> includes ) {
        return new DocumentType( Streams.mapWithIndex( includes.entrySet().stream(), ( e, i ) -> new AlgDataTypeFieldImpl( -1L, e.getKey() == null ? "" : e.getKey(), (int) i, e.getValue().getType() ) ).collect( Collectors.toList() ) );
    }


    public static AlgDataType ofCrossRelational() {
        return new AlgRecordType( List.of(
                new AlgDataTypeFieldImpl( -1L, "d", 0, AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.DOCUMENT ) )
        ) );
    }


    public AlgDataType ofExcludes( List<String> excludes ) {
        return new DocumentType( fixed, excludes );
    }


    private String computeDigest() {
        assert fixed != null;
        return DataModel.DOCUMENT.name();
    }


    @Override
    public boolean isStruct() {
        return true;
    }


    @Override
    public AlgDataType asRelational() {
        return ofRelational();
    }


    @Override
    public AlgDataType asDocument() {
        return this;
    }


    @Override
    public List<AlgDataTypeField> getFields() {
        return fixed;
    }


    @Override
    public List<String> getFieldNames() {
        return getFields().stream().map( AlgDataTypeField::getName ).toList();
    }


    @Override
    public List<Long> getFieldIds() {
        return fixed.stream().map( AlgDataTypeField::getId ).toList();
    }


    @Override
    public int getFieldCount() {
        return getFields().size();
    }


    @Override
    public AlgDataTypeField getField( String fieldName, boolean caseSensitive, boolean elideRecord ) {
        // everything we ask a document for is there
        int index = getFieldNames().indexOf( fieldName );
        if ( index >= 0 ) {
            return getFields().get( index );
        }
        AlgDataTypeFieldImpl added = new AlgDataTypeFieldImpl( -1L, fieldName, getFieldCount(), new DocumentType() );
        fixed.add( added );
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
        return PolyType.DOCUMENT;
    }


    @Override
    public String getFullTypeString() {
        return digest;
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
