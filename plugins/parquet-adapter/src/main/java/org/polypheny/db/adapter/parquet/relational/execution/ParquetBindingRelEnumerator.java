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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

/**
 * Root-table scanner that
 * reads root table rows and extracts selected columns by path
 */
public class ParquetBindingRelEnumerator implements Enumerator<PolyValue[]> {

    private final ParquetSourceReader reader;
    private final List<ParquetColumnBinding> columnBindings;
    private final ParquetRelValueExtractor valueExtractor = new ParquetRelValueExtractor();
    private PolyValue[] current;


    public ParquetBindingRelEnumerator( Source source, AtomicBoolean cancelFlag, List<ParquetColumnBinding> columnBindings ) {
        // read full root rows from the Parquet file
        this.reader = new ParquetSourceReader( source, cancelFlag, null, List.of() );
        this.columnBindings = List.copyOf( columnBindings );
    }


    @Override
    public PolyValue[] current() {
        return current;
    }


    @Override
    public boolean moveNext() {
        try {
            Group group = reader.next();
            if ( group == null ) {
                current = null;
                return false;
            }

            current = new PolyValue[columnBindings.size()];
            for ( int i = 0; i < columnBindings.size(); i++ ) {
                // extract value by path
                // if the binding path is: List.of("shipping_address", "city") it walks: "root -> shipping_address -> city"
                current[i] = extractPathValue( group, columnBindings.get( i ).sourcePathElements() );
            }
            return true;
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error while reading parquet data using column bindings", e );
        }
    }


    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() {
        try {
            reader.close();
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Error closing parquet reader", e );
        }
    }


    private PolyValue extractPathValue( Group root, List<String> sourcePath ) {
        if ( sourcePath.isEmpty() ) {
            return PolyNull.NULL;
        }

        Group currentGroup = root;
        GroupType currentType = root.getType();
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
