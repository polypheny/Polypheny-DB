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

package org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.type.PolyType;

import java.util.Optional;

@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Column {
    Result result;
    String name;
    PolyType polyType;
    long id;
    Alias alias;

    public String nSplit( int i ) {
        return name.split("\\.")[i];
    }

    public Optional<Alias> getAlias() {
        if ( this.alias == null ) {
            return Optional.empty();
        } else {
            return Optional.of( this.alias );
        }
    }

    public static Column from( Result result, CatalogColumn column ) {
        Column resultColumn = new Column();
        resultColumn.setName( column.getTableName() + "." + column.name );
        resultColumn.setPolyType( column.type );
        resultColumn.setResult( result );
        resultColumn.setId( column.id );
        resultColumn.setAlias( null );
        return resultColumn;
    }

    public static Column from( Result result, Column column ) {
        Column resultColumn = new Column();
        resultColumn.setName( column.getName() );
        resultColumn.setId( column.getId() );
        result.getAlias().ifPresentOrElse( resultColumn::setAlias, () -> resultColumn.setAlias( null ) );
        resultColumn.setPolyType( column.getPolyType() );
        resultColumn.setResult( result );
        return resultColumn;
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Column)) {
            return false;
        }
        return getName().equals( ((Column) obj).getName() );
    }
}
