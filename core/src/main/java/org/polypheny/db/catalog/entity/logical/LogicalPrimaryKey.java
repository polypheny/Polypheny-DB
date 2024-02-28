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


import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serial;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.PolyObject;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;


@Value
public class LogicalPrimaryKey extends LogicalKey {

    @Serialize
    public LogicalKey key;


    public LogicalPrimaryKey( @Deserialize("key") @NonNull final LogicalKey key ) {
        super(
                key.id,
                key.entityId,
                key.namespaceId,
                key.columnIds,
                EnforcementTime.ON_QUERY );

        this.key = key;
    }


    // Used for creating ResultSets
    public List<LogicalPrimaryKeyColumn> getCatalogPrimaryKeyColumns() {
        int i = 1;
        List<LogicalPrimaryKeyColumn> list = new LinkedList<>();
        for ( String columnName : getColumnNames() ) {
            list.add( new LogicalPrimaryKeyColumn( id, i++, columnName ) );
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


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        if ( !super.equals( o ) ) {
            return false;
        }

        LogicalPrimaryKey that = (LogicalPrimaryKey) o;

        return Objects.equals( key, that.key );
    }


    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (key != null ? key.hashCode() : 0);
        return result;
    }


    // Used for creating ResultSets
    @RequiredArgsConstructor
    public static class LogicalPrimaryKeyColumn implements PolyObject {

        @Serial
        private static final long serialVersionUID = -2669773639977732201L;

        private final long pkId;

        private final int keySeq;

        private final String columnName;


        @Override
        public PolyValue[] getParameterArray() {
            return Catalog.snapshot().rel().getPrimaryKey( pkId ).orElseThrow().getParameterArray( columnName, keySeq );
        }


        @Override
        public boolean equals( Object o ) {
            if ( this == o ) {
                return true;
            }
            if ( o == null || getClass() != o.getClass() ) {
                return false;
            }

            LogicalPrimaryKeyColumn that = (LogicalPrimaryKeyColumn) o;

            if ( pkId != that.pkId ) {
                return false;
            }
            if ( keySeq != that.keySeq ) {
                return false;
            }
            return Objects.equals( columnName, that.columnName );
        }


        @Override
        public int hashCode() {
            int result = (int) (pkId ^ (pkId >>> 32));
            result = 31 * result + keySeq;
            result = 31 * result + (columnName != null ? columnName.hashCode() : 0);
            return result;
        }


        @RequiredArgsConstructor
        public static class PrimitiveCatalogPrimaryKeyColumn {

            public final String tableCat;
            public final String tableSchem;
            public final String tableName;
            public final String columnName;
            public final int keySeq;
            public final String pkName;

        }

    }

}
