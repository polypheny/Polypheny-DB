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

package org.polypheny.db.algebra.polyalg.arguments;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import lombok.Getter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.Parameter;

/**
 * Represents the parameters with corresponding values (PolyAlgArg) for an AlgNode instance.
 * It is used as an intermediary representation when serializing or parsing between AlgNodes and PolyAlgebra.
 */
public class PolyAlgArgs {

    @Getter
    private final PolyAlgDeclaration decl;
    private final Map<Parameter, PolyAlgArg> args = new HashMap<>();


    public PolyAlgArgs( PolyAlgDeclaration declaration ) {
        this.decl = declaration;
    }


    public String serializeArguments( AlgNode context, List<String> inputFieldNames ) {
        StringJoiner joiner = new StringJoiner( ", ", "[", "]" );

        for ( Parameter p : decl.posParams ) {
            assert args.containsKey( p );
            PolyAlgArg arg = getArg( p );
            joiner.add( arg.toPolyAlg( context, inputFieldNames ) );
        }
        for ( Parameter p : decl.kwParams ) {
            if ( args.containsKey( p ) ) {
                PolyAlgArg arg = getArg( p );
                String value = arg.toPolyAlg( context, inputFieldNames );
                if ( !p.getDefaultValue().equals( value ) ) {
                    joiner.add( p.getName() + "=" + value );
                }
            }
        }
        return joiner.toString();
    }


    public PolyAlgArgs put( String name, PolyAlgArg arg ) {
        return put( decl.getParam( name ), arg );
    }


    public PolyAlgArgs put( int pos, PolyAlgArg arg ) {
        return put( decl.getPos( pos ), arg );
    }


    private PolyAlgArgs put( Parameter p, PolyAlgArg arg ) {
        assert p.getType() == arg.getType();

        args.put( p, arg );
        return this;
    }


    private PolyAlgArg getArg( Parameter p ) {
        return args.get( p );
    }


    public PolyAlgArg getArg( String name ) {
        return getArg( decl.getParam( name ) );
    }


    public PolyAlgArg getArg( int pos ) {
        return getArg( decl.getPos( pos ) );
    }


    public <T extends PolyAlgArg> T getArg( String name, Class<T> type ) {
        return getArg( decl.getParam( name ), type );
    }


    public <T extends PolyAlgArg> T getArg( int pos, Class<T> type ) {
        return getArg( decl.getPos( pos ), type );
    }


    private <T extends PolyAlgArg> T getArg( Parameter p, Class<T> type ) {
        PolyAlgArg arg = getArg( p );
        assert type.isInstance( arg );
        return type.cast( arg );
    }


}
