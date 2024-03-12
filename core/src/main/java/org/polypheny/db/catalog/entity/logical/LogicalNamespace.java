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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.With;
import lombok.experimental.NonFinal;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.PolyObject;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;


@EqualsAndHashCode(callSuper = false)
@With
@Value
@NonFinal // for testing
public class LogicalNamespace implements PolyObject, Comparable<LogicalNamespace> {

    @Serial
    private static final long serialVersionUID = 3090632164988970558L;

    @Serialize
    public long id;
    @Serialize
    @Getter
    public String name;
    @Serialize
    @Getter
    @EqualsAndHashCode.Exclude
    public DataModel dataModel;
    @Serialize
    public boolean caseSensitive;


    public LogicalNamespace(
            @Deserialize("id") final long id,
            @Deserialize("name") @NonNull final String name,
            @Deserialize("dataModel") @NonNull final DataModel dataModel,
            @Deserialize("caseSensitive") boolean caseSensitive ) {
        this.id = id;
        this.name = name;
        this.dataModel = dataModel;
        this.caseSensitive = caseSensitive;
    }


    // Used for creating ResultSets
    @Override
    public PolyValue[] getParameterArray() {
        return new PolyValue[]{
                PolyString.of( name ),
                PolyString.of( Catalog.DATABASE_NAME ),
                PolyString.of( Catalog.USER_NAME ),
                PolyString.of( PolyObject.getEnumNameOrNull( dataModel ) ) };
    }


    @Override
    public int compareTo( LogicalNamespace o ) {
        if ( o != null ) {
            return (int) (this.id - o.id);
        }

        return -1;
    }


    public record PrimitiveCatalogSchema(String tableSchem, String tableCatalog, String owner, String schemaType) {

    }

}
