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

package org.polypheny.db.rex;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.polypheny.db.algebra.type.AlgDataType;

@EqualsAndHashCode(callSuper = false)
@Value
public class RexNameRef extends RexVariable {

    List<String> names;


    /**
     * Regular expression, that references an input field by name
     */
    public RexNameRef( List<String> names, AlgDataType type ) {
        super( String.join( ".", names ), type );
        this.names = names;
    }


    public static RexNameRef create( List<String> names, AlgDataType type ) {
        return new RexNameRef( names, type );
    }


    public static RexNameRef create( String name, AlgDataType type ) {
        return new RexNameRef( List.of( name.split( "\\." ) ), type );
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
