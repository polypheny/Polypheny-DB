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

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;
import org.polypheny.db.nodes.IntervalQualifier;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Collation;

public class GraphType implements Serializable, AlgDataType, AlgDataTypeFamily, AlgDataTypeField {

    @Override
    public String getKey() {
        return null;
    }


    @Override
    public AlgDataType getValue() {
        return null;
    }


    @Override
    public AlgDataType setValue( AlgDataType value ) {
        return null;
    }


    @Override
    public boolean isStruct() {
        return false;
    }


    @Override
    public List<AlgDataTypeField> getFieldList() {
        return null;
    }


    @Override
    public List<String> getFieldNames() {
        return null;
    }


    @Override
    public int getFieldCount() {
        return 0;
    }


    @Override
    public StructKind getStructKind() {
        return null;
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
        return null;
    }


    @Override
    public boolean isDynamicStruct() {
        return false;
    }


    @Override
    public String getName() {
        return null;
    }


    @Override
    public String getPhysicalName() {
        return null;
    }


    @Override
    public int getIndex() {
        return 0;
    }


    @Override
    public AlgDataType getType() {
        return null;
    }


    @Override
    public boolean isDynamicStar() {
        return false;
    }

}
