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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Collation;

@Data
public class GraphType implements Serializable, AlgDataType, AlgDataTypeFamily {

    public static final String GRAPH_ID = "_id";
    public static final String GRAPH_PROPERTIES = "properties";
    public static final String GRAPH_LABELS = "labels";

    public static final int ID_SIZE = 36;

    public final List<AlgDataTypeField> fixedFields;

    public String digest;


    public GraphType( List<AlgDataTypeField> fixedFields ) {
        this.fixedFields = fixedFields;
        this.digest = computeDigest();
    }


    public static GraphType of() {
        return new GraphType( List.of( new AlgDataTypeFieldImpl( -1L, GRAPH_ID, 0, AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.CHAR, ID_SIZE ) ) ) );
    }


    public static AlgDataType ofRelational() {
        return new AlgRecordType( List.of(
                new AlgDataTypeFieldImpl( -1L, GRAPH_ID, 0, AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.VARCHAR, ID_SIZE ) ),
                new AlgDataTypeFieldImpl( -1L, GRAPH_PROPERTIES, 1, AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.TEXT ) ),
                new AlgDataTypeFieldImpl( -1L, GRAPH_LABELS, 2, AlgDataTypeFactory.DEFAULT.createArrayType( AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.TEXT ), -1 ) )
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
    public AlgDataType asRelational() {
        return ofRelational();
    }


    @Override
    public AlgDataType asDocument() {
        throw new UnsupportedOperationException();
    }


    @Override
    public AlgDataType asGraph() {
        return this;
    }


    @Override
    public List<AlgDataTypeField> getFields() {
        return fixedFields;
    }


    @Override
    public List<String> getFieldNames() {
        return getFields().stream().map( AlgDataTypeField::getName ).collect( Collectors.toList() );
    }


    @Override
    public List<Long> getFieldIds() {
        return fixedFields.stream().map( AlgDataTypeField::getId ).collect( Collectors.toList() );
    }


    @Override
    public int getFieldCount() {
        return fixedFields.size();
    }


    @Override
    public StructKind getStructKind() {
        return StructKind.SEMI;
    }


    @Override
    public AlgDataTypeField getField( String fieldName, boolean caseSensitive, boolean elideRecord ) {
        return null;
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
        return PolyType.GRAPH;
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
        return null;
    }


    @Override
    public boolean isDynamicStruct() {
        return false;
    }


}
