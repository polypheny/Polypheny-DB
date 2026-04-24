/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.relational.execution;

import java.util.List;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.execution.AbstractParquetValueExtractor;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Extracts relational values by Parquet source path.
 * Used by binding-aware root scans and generated nested-table scans.
 */
public class ParquetPathValueExtractor extends AbstractParquetValueExtractor {

    private final ParquetRelValueExtractor valueExtractor = new ParquetRelValueExtractor();


    @Override
    public PolyValue extractValue( Group group, int index, Type type ) {
        return valueExtractor.extractValue( group, index, type );
    }


    @Override
    public PolyValue extractValue( Group group, List<String> sourcePath ) {
        if ( sourcePath.isEmpty() ) {
            return PolyNull.NULL;
        }

        Group currentGroup = group;
        GroupType currentType = group.getType();

        for ( int i = 0; i < sourcePath.size(); i++ ) {
            int index = fieldIndex( currentType, sourcePath.get( i ) );
            if ( index < 0 || currentGroup.getFieldRepetitionCount( index ) == 0 ) {
                return PolyNull.NULL;
            }

            Type fieldType = currentType.getType( index );
            if ( i == sourcePath.size() - 1 ) {
                return valueExtractor.extractValue( currentGroup, index, fieldType );
            }

            if ( fieldType.isPrimitive() || currentGroup.getFieldRepetitionCount( index ) > 1 ) {
                return PolyNull.NULL;
            }
            currentGroup = currentGroup.getGroup( index, 0 );
            currentType = fieldType.asGroupType();
        }
        return PolyNull.NULL;
    }


    private int fieldIndex( GroupType groupType, String fieldName ) {
        for ( int i = 0; i < groupType.getFieldCount(); i++ ) {
            if ( groupType.getType( i ).getName().equals( fieldName ) ) {
                return i;
            }
        }
        return -1;
    }

}
