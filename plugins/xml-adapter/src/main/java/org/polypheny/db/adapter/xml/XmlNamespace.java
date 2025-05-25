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

package org.polypheny.db.adapter.xml;

import java.util.Optional;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.Namespace;

@Getter
final class XmlNamespace extends Namespace {

    private final String name;


    XmlNamespace( String name, long id, long adapterId ) {
        super( id, adapterId );
        this.name = name;
    }


    @Override
    protected @Nullable Convention getConvention() {
        return null;
    }


    @Override
    public @NotNull <C> Optional<C> unwrap( Class<C> aClass ) {
        return super.unwrap( aClass );
    }


    @Override
    public <C> @NotNull C unwrapOrThrow( Class<C> aClass ) {
        return super.unwrapOrThrow( aClass );
    }

}
