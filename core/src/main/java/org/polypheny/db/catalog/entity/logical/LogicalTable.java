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

package org.polypheny.db.catalog.entity.logical;


import com.google.common.collect.ImmutableList;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.With;
import lombok.experimental.NonFinal;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeImpl;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.partition.properties.PartitionProperty;
import org.polypheny.db.schema.ColumnStrategy;

@Value
@With
@EqualsAndHashCode(callSuper = false)
@NonFinal
public class LogicalTable extends CatalogEntity implements Comparable<LogicalTable>, Logical {

    private static final long serialVersionUID = 4653390333258552102L;

    @Serialize
    public long id;
    @Serialize
    public String name;
    @Serialize
    public ImmutableList<Long> fieldIds;
    @Serialize
    public long namespaceId;
    @Serialize
    public int ownerId;
    @Serialize
    public EntityType entityType;
    @Serialize
    public Long primaryKey;
    @Serialize
    public boolean modifiable;
    @Serialize
    public PartitionProperty partitionProperty;
    @Serialize
    public ImmutableList<Integer> dataPlacements;
    @Serialize
    public ImmutableList<Long> connectedViews;


    public LogicalTable(
            final long id,
            @NonNull final String name,
            final ImmutableList<Long> fieldIds,
            final long namespaceId,
            final int ownerId,
            @NonNull final EntityType type,
            final Long primaryKey,
            @NonNull final List<Integer> dataPlacements,
            boolean modifiable,
            PartitionProperty partitionProperty ) {
        this( id, name, fieldIds, namespaceId, ownerId, type, primaryKey, dataPlacements, modifiable, partitionProperty, ImmutableList.of() );
    }


    public LogicalTable(
            @Deserialize("id") final long id,
            @Deserialize("name") @NonNull final String name,
            @Deserialize("fieldIds") final List<Long> fieldIds,
            @Deserialize("namespaceId") final long namespaceId,
            @Deserialize("ownerId") final int ownerId,
            @Deserialize("type") @NonNull final EntityType type,
            @Deserialize("primaryKey") final Long primaryKey,
            @Deserialize("dataPlacements") @NonNull final List<Integer> dataPlacements,
            @Deserialize("modifiable") boolean modifiable,
            @Deserialize("partitionProperty") PartitionProperty partitionProperty,
            @Deserialize("connectedViews") List<Long> connectedViews ) {
        super( id, name, type, NamespaceType.RELATIONAL );
        this.id = id;
        this.name = name;
        this.fieldIds = ImmutableList.copyOf( fieldIds );
        this.namespaceId = namespaceId;
        this.ownerId = ownerId;
        this.entityType = type;
        this.primaryKey = primaryKey;
        this.modifiable = modifiable;

        this.partitionProperty = partitionProperty;

        this.connectedViews = ImmutableList.copyOf( connectedViews );

        this.dataPlacements = ImmutableList.copyOf( dataPlacements );

        if ( type == EntityType.ENTITY && !modifiable ) {
            throw new RuntimeException( "Tables of table type TABLE must be modifiable!" );
        }
    }



    @SneakyThrows
    public String getNamespaceName() {
        return Catalog.getInstance().getNamespace( namespaceId ).name;
    }


    @SneakyThrows
    public NamespaceType getNamespaceType() {
        return Catalog.getInstance().getNamespace( namespaceId ).namespaceType;
    }


    @SneakyThrows
    public String getOwnerName() {
        return Catalog.getInstance().getUser( ownerId ).name;
    }


    @SneakyThrows
    public List<String> getColumnNames() {
        Catalog catalog = Catalog.getInstance();
        List<String> fieldNames = new LinkedList<>();
        for ( long fieldId : fieldIds ) {
            fieldNames.add( catalog.getColumn( fieldId ).name );
        }
        return fieldNames;
    }


    // Used for creating ResultSets
    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[]{
                getNamespaceName(),
                name,
                entityType.name(),
                "",
                null,
                null,
                null,
                null,
                null,
                getOwnerName()

        };
    }


    @Override
    public int compareTo( LogicalTable o ) {
        if ( o != null ) {
            int comp = (int) (this.namespaceId - o.namespaceId);
            if ( comp == 0 ) {
                return (int) (this.id - o.id);
            } else {
                return comp;
            }
        }
        return -1;
    }


    @Override
    public AlgDataType getRowType() {
        final AlgDataTypeFactory.Builder fieldInfo = AlgDataTypeFactory.DEFAULT.builder();

        for ( Long id : fieldIds ) {
            LogicalColumn logicalColumn = Catalog.getInstance().getColumn( id );
            AlgDataType sqlType = logicalColumn.getAlgDataType( AlgDataTypeFactory.DEFAULT );
            fieldInfo.add( logicalColumn.name, null, sqlType ).nullable( logicalColumn.nullable );
        }

        return AlgDataTypeImpl.proto( fieldInfo.build() ).apply( AlgDataTypeFactory.DEFAULT );
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( Expressions.call( Catalog.class, "getInstance" ), "getLogicalTable", Expressions.constant( id ) );
    }


    public List<ColumnStrategy> getColumnStrategies() {
        return null;
    }


    @RequiredArgsConstructor
    public static class PrimitiveCatalogTable {

        public final String tableCat;
        public final String tableSchem;
        public final String tableName;
        public final String tableType;
        public final String remarks;
        public final String typeCat;
        public final String typeSchem;
        public final String typeName;
        public final String selfReferencingColName;
        public final String refGeneration;
        public final String owner;

    }

}
