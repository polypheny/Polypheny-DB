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

package org.polypheny.db.adapter;


import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.ExtensionPoint;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.InsertSourceOrdinals;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.catalogs.AdapterCatalog;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.IndexCategory;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;

@Slf4j
public abstract class DataStore<S extends AdapterCatalog> extends Adapter<S> implements Modifiable, ExtensionPoint {

    protected final transient Catalog catalog = Catalog.getInstance();
    @Getter
    private final boolean persistent;


    public DataStore( final long adapterId, final String uniqueName, final Map<String, String> settings, final DeployMode mode, final boolean persistent, S storeCatalog ) {
        super( adapterId, uniqueName, settings, mode, storeCatalog );
        this.persistent = persistent;

        informationPage.setLabel( "Stores" );
    }


    public abstract List<IndexMethodModel> getAvailableIndexMethods();

    public abstract IndexMethodModel getDefaultIndexMethod();

    public abstract List<FunctionalIndexInfo> getFunctionalIndexes( LogicalTable catalogTable );


    public boolean supportsReadInsertSource() {
        return false;
    }


    public static boolean isInsertSourceAssignmentType( PolyType type ) {
        return PolyType.NUMERIC_TYPES.contains( type ) || type == PolyType.TIME || type == PolyType.TIMESTAMP;
    }


    /**
     * Builds a read-only plan for a native INSERT's source, with destination assignments
     * applied by the store before the coordinator materializes the rows.
     */
    public Optional<AlgNode> readInsertSource( AlgNode physicalInsert, List<AlgDataTypeField> targetFields, Set<Long> nativeTemporalFields ) {
        return Optional.empty();
    }


    /** Adds provenance to a native INSERT preview or a source read by its streamer. */
    public Optional<InsertSourceOrdinals> markInsertSource( AlgNode source, int firstOrdinal, List<AlgDataTypeField> identityFields ) {
        return Optional.empty();
    }


    /** Converts numeric assignments before the coordinator routes or indexes the resulting keys. */
    public List<PolyValue> castNumericAssignments( Transaction transaction, AlgCluster cluster, AlgDataType target, List<NumericAssignment> assignments ) {
        return assignments.stream().map( assignment -> {
            PolyValue value = assignment.value();
            if ( value == null || value.isNull() ) {
                return value;
            }
            return switch ( target.getPolyType() ) {
                case TINYINT, SMALLINT, INTEGER -> PolyInteger.of( value.asNumber().intValue() );
                case DECIMAL -> value;
                case BIGINT -> PolyLong.of( value.asNumber().bigDecimalValue().setScale( 0, RoundingMode.DOWN ).longValueExact() );
                case REAL, FLOAT -> PolyFloat.of( value.asNumber().floatValue() );
                case DOUBLE -> PolyDouble.of( value.asNumber().doubleValue() );
                default -> throw new IllegalArgumentException( "Unsupported numeric assignment target " + target );
            };
        } ).toList();
    }


    /** Applies the store's temporal precision before a materialized INSERT writes or reports values. */
    public List<PolyValue> castTemporalAssignments( Transaction transaction, AlgCluster cluster, AlgDataType target, boolean nativePrecision,
            List<TemporalAssignment> assignments ) {
        return assignments.stream().map( TemporalAssignment::value ).toList();
    }


    public record NumericAssignment( PolyValue value, AlgDataType inputType, NumericExpression expression ) {
    }


    public record TemporalAssignment( PolyValue value, RexNode expression, Map<Long, PolyValue> parameters ) {
    }


    public record NumericExpression( RexNode node, Map<Long, PolyValue> parameters ) {
    }


    public record IndexMethodModel(
            @JsonProperty String name,
            @JsonProperty String displayName,
            @JsonProperty IndexCategory category,
            @JsonProperty List<IndexParameterModel> parameters
    ) {

        public IndexMethodModel( String name, String displayName ) {
            this( name, displayName, IndexCategory.REGULAR, List.of() );
        }

    }


    public record IndexParameterModel(
            @JsonProperty String name,
            @JsonProperty String displayName,
            @JsonProperty String type,          //INTEGER, BOOLEAN, ENUM
            @JsonProperty List<String> options,
            @JsonProperty String defaultValue
    ) {

    }


    public record FunctionalIndexInfo( List<Long> columnIds, String methodDisplayName ) {

        public List<String> getColumnNames() {
            List<String> columnNames = new ArrayList<>( columnIds.size() );
            for ( long columnId : columnIds ) {
                columnNames.add( Catalog.snapshot().rel().getColumn( columnId ).orElseThrow().name );
            }
            return columnNames;
        }

    }

}
