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

import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.shared.execution.ParquetValueExtractor;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Filter functionality on Adapter Level
 * works with group (row)
 */
public class ParquetGroupFilterEvaluator extends FilterEvaluator<Group> {

    protected final MessageType schema;
    protected final ParquetValueExtractor valueExtractor;


    public ParquetGroupFilterEvaluator( MessageType schema, ParquetValueExtractor valueExtractor ) {
        this.schema = schema;
        this.valueExtractor = valueExtractor;
    }


    public ParquetValueExtractor valueExtractor() {
        return valueExtractor;
    }


    public MessageType schema() {
        return schema;
    }


    @Override
    protected Boolean evaluateLeaf( Group group, ParquetAdapterFilter filter ) {
        if ( !canApplyFilter( group, filter ) ) {
            return null;
        }
        if ( !filterHasValue( group, filter ) ) {
            return matchesValue( PolyNull.NULL, filter.operator(), filter.polyValue() );
        }
        return matchesValue( extractValue( group, filter ), filter.operator(), filter.polyValue() );
    }


    protected boolean canApplyFilter( Group group, ParquetAdapterFilter filter ) {
        if ( filter.columnIndex() < 0 ) {
            return false;
        }
        if ( filter.polyValue() == null && !isNullCheck( filter.operator() ) ) {
            return false;
        }
        return !filter.pathElements().isEmpty() || filter.columnIndex() < schema.getFieldCount();
    }


    protected boolean filterHasValue( Group group, ParquetAdapterFilter filter ) {
        return !filter.pathElements().isEmpty() || group.getFieldRepetitionCount( filter.columnIndex() ) > 0;
    }


    protected PolyValue extractValue( Group group, ParquetAdapterFilter filter ) {
        if ( filter.pathElements().isEmpty() ) {
            Type field = schema.getType( filter.columnIndex() );
            return valueExtractor.extractValue( group, filter.columnIndex(), field );
        }
        return valueExtractor.extractValue( group, filter.pathElements() );
    }


    private boolean isNullCheck( Kind operator ) {
        return operator == Kind.IS_NULL || operator == Kind.IS_NOT_NULL;
    }

}
