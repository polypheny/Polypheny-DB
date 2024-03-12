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

package org.polypheny.db.catalog.entity.logical;


import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serial;
import java.util.LinkedList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.PolyObject;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;


@Value
@EqualsAndHashCode(callSuper = false)
public class LogicalPrimaryKey extends LogicalKey {

    @Serialize
    public LogicalKey key;


    public LogicalPrimaryKey( @Deserialize("key") @NonNull final LogicalKey key ) {
        super(
                key.id,
                key.entityId,
                key.namespaceId,
                key.fieldIds,
                EnforcementTime.ON_QUERY );

        this.key = key;
    }


    // Used for creating ResultSets
    public List<LogicalPrimaryKeyField> getCatalogPrimaryKeyColumns() {
        int i = 1;
        List<LogicalPrimaryKeyField> list = new LinkedList<>();
        for ( String columnName : getFieldNames() ) {
            list.add( new LogicalPrimaryKeyField( id, i++, columnName ) );
        }
        return list;
    }


    public PolyValue[] getParameterArray( String columnName, int keySeq ) {
        return new PolyValue[]{
                PolyString.of( Catalog.DATABASE_NAME ),
                PolyString.of( getSchemaName() ),
                PolyString.of( getTableName() ),
                PolyString.of( columnName ),
                PolyInteger.of( keySeq ), null };
    }


    // Used for creating ResultSets
    @RequiredArgsConstructor
    public static class LogicalPrimaryKeyField implements PolyObject {

        @Serial
        private static final long serialVersionUID = -2669773639977732201L;

        private final long pkId;

        private final int keySeq;

        private final String fieldName;


        @Override
        public PolyValue[] getParameterArray() {
            return Catalog.snapshot().rel().getPrimaryKey( pkId ).orElseThrow().getParameterArray( fieldName, keySeq );
        }


        public record PrimitiveCatalogPrimaryKeyColumn(String tableCat, String tableSchem, String tableName, String columnName, int keySeq, String pkName) {

        }

    }

}
