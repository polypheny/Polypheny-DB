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

package org.polypheny.db.rex;

import java.util.List;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;

@EqualsAndHashCode(callSuper = false)
@Value
public class RexNameRef extends RexVariable {

    @NotNull
    public List<String> names;
    @Nullable
    Integer index;


    public Optional<Integer> getIndex() {
        return Optional.ofNullable( index );
    }


    /**
     * Regular expression, that references an input field by name
     */
    public RexNameRef( List<String> names, @Nullable Integer index, AlgDataType type ) {
        super( String.join( ".", names ), type );
        this.names = names.stream().filter( n -> !n.isEmpty() ).toList();
        this.index = index;
    }


    public RexNameRef( String name, @Nullable Integer index, AlgDataType type ) {
        this( List.of( name.split( "\\." ) ), index, type );
    }


    public static RexNameRef create( List<String> names, @Nullable Integer index, AlgDataType type ) {
        return new RexNameRef( names, index, type );
    }


    public static RexNameRef create( String name, @Nullable Integer index, AlgDataType type ) {
        return new RexNameRef( List.of( name.split( "\\." ) ), index, type );
    }


    @Override
    public Kind getKind() {
        return Kind.NAME_INDEX_REF;
    }


    @Override
    public <R> R accept( RexVisitor<R> visitor ) {
        return visitor.visitNameRef( this );
    }


    @Override
    public <R, P> R accept( RexBiVisitor<R, P> visitor, P arg ) {
        return visitor.visitNameRef( this, arg );
    }




}
