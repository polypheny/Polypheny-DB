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

package org.polypheny.db.adapter.parquet.shared.filter;

import java.util.List;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetPathValueExtractor;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.shared.execution.ParquetValueExtractor;
import org.polypheny.db.adapter.parquet.shared.execution.VirtualGroup;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Evaluates filter of nested table group.
 */
public class ParquetNestedFilterEvaluator extends ParquetGroupFilterEvaluator {

    private final List<String> tablePath;
    private final List<ParquetColumnBinding> columnBindings;


    public ParquetNestedFilterEvaluator( MessageType schema, ParquetValueExtractor valueExtractor, List<String> tablePath, List<ParquetColumnBinding> columnBindings ) {
        super( schema, valueExtractor );
        this.tablePath = tablePath;
        this.columnBindings = columnBindings;
    }


    @Override
    protected PolyValue extractValue( Group group, ParquetAdapterFilter filter ) {
        var pathValueExtractor = (ParquetPathValueExtractor) valueExtractor;
        var virtualGroup = (VirtualGroup) group;
        if ( filter.columnIndex() < 0 || filter.columnIndex() >= columnBindings.size() ) {
            return PolyNull.NULL;
        }
        var binding = columnBindings.get( filter.columnIndex() );
        return pathValueExtractor.extractValue( virtualGroup, binding, tablePath );
    }

}
