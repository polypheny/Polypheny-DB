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
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.execution.CombinedGroup;
import org.polypheny.db.adapter.parquet.shared.execution.VirtualGroup;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.PolyValue;

/**
 * Evaluates filter on {@link CombinedGroup} containing row from joined nested tables.
 */
public class ParquetNestedJoinFilterEvaluator extends ParquetGroupFilterEvaluator {

    private final ParquetTableBinding parentBinding;
    private final List<ParquetColumnBinding> parentColumns;
    private final boolean leftIsParent;


    public ParquetNestedJoinFilterEvaluator( MessageType schema, ParquetPathValueExtractor valueExtractor, ParquetTableBinding parentBinding, List<ParquetColumnBinding> parentColumns, boolean leftIsParent ) {
        super( schema, valueExtractor );
        this.parentBinding = parentBinding;
        this.parentColumns = parentColumns;
        this.leftIsParent = leftIsParent;
    }


    @Override
    protected PolyValue extractValue( Group group, ParquetAdapterFilter filter ) {
        ParquetPathValueExtractor pathValueExtractor = (ParquetPathValueExtractor) valueExtractor;
        if ( group instanceof CombinedGroup combinedGroup ) {
            return pathValueExtractor.extractValue(
                    combinedGroup.groupForField( filter.columnIndex(), leftIsParent ),
                    combinedGroup.bindingForField( filter.columnIndex(), leftIsParent ),
                    combinedGroup.tablePathForField( filter.columnIndex(), leftIsParent )
            );
        }
        // If this is not a combined group then the filtering is being done on parent table only.
        return pathValueExtractor.extractValue(
                (VirtualGroup) group,
                parentColumns.get( filter.columnIndex() ),
                parentBinding.sourcePathElements()
        );
    }


    /**
     * Checks if a filter is valid and can be applied on the group.
     *
     * @param group a group to check
     * @param filter a filter to validate
     * @return true if the filter is valid and false otherwise.
     */
    @Override
    protected boolean canApplyFilter( Group group, ParquetAdapterFilter filter ) {
        if ( filter.columnIndex() < 0 ) {
            return false;
        }
        if ( filter.polyValue() == null && !isNullCheck( filter.operator() ) ) {
            return false;
        }
        if ( group instanceof CombinedGroup combinedGroup ) {
            return filter.columnIndex() < combinedGroup.fieldCount();
        }
        return filter.columnIndex() < parentColumns.size();
    }


    /**
     * Checks if the group contains a value.
     *
     * @param group a group to check.
     * @param filter a filter containing column index which value is checked
     * @return true if the group has a value at the filter column index and false otherwise.
     */
    @Override
    protected boolean filterHasValue( Group group, ParquetAdapterFilter filter ) {
        CombinedGroup combinedGroup = (CombinedGroup) group;
        return !combinedGroup.isNullField( filter.columnIndex(), leftIsParent );
    }


    private boolean isNullCheck( Kind operator ) {
        return operator == Kind.IS_NULL || operator == Kind.IS_NOT_NULL;
    }

}
