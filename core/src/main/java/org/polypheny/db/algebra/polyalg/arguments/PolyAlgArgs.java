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
                if ( !p.getDefaultAsPolyAlg( context, inputFieldNames ).equals( value ) ) {
                    joiner.add( p.getName() + "=" + value );
                }
            }
        }
        return joiner.toString();
    }


    public boolean contains( Parameter p ) {
        return args.containsKey( p );
    }


    /**
     * Validates if all required parameters are present and adds default values for missing keyword parameters if specified.
     *
     * @param addDefaultOnMissingKw {@code true} to add default values for missing keyword parameters
     * @return {@code true} if all required parameters are present, {@code false} otherwise.
     */
    public boolean validate( boolean addDefaultOnMissingKw ) {
        for ( Parameter p : decl.posParams ) {
            if ( !args.containsKey( p ) ) {
                return false;
            }
        }
        for ( Parameter p : decl.kwParams ) {
            if ( !args.containsKey( p ) ) {
                if ( addDefaultOnMissingKw ) {
                    put( p, p.getDefaultValue() );
                } else {
                    return false;
                }
            }
        }
        return true;
    }


    public PolyAlgArgs put( String name, PolyAlgArg arg ) {
        return put( decl.getParam( name ), arg );
    }


    public PolyAlgArgs put( int pos, PolyAlgArg arg ) {
        return put( decl.getPos( pos ), arg );
    }


    public PolyAlgArgs putWithCheck( Parameter p, PolyAlgArg arg ) {
        if ( decl.containsParam( p ) ) {
            put( p, arg );
        }
        return this;
    }


    /**
     * Inserts an argument for the specified parameter.
     * If you are not sure whether that parameter belongs to the declaration, it is better to use {@link #putWithCheck}.
     *
     * @return this instance for chaining calls
     */
    public PolyAlgArgs put( Parameter p, PolyAlgArg arg ) {
        assert p.isCompatible( arg.getType() );

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


    public <T extends PolyAlgArg> ListArg<T> getListArg( int pos, Class<T> type ) {
        ListArg<?> listArg = getArg( pos, ListArg.class );
        assert listArg.isEmpty() || type.isInstance( listArg.getArgs().get( 0 ) ); // an empty ListArg is of type EMPTY_LIST
        return (ListArg<T>) listArg;
    }


    public <T extends PolyAlgArg> ListArg<T> getListArg( String name, Class<T> type ) {
        ListArg<?> listArg = getArg( name, ListArg.class );
        assert listArg.isEmpty() || type.isInstance( listArg.getArgs().get( 0 ) ); // an empty ListArg is of type EMPTY_LIST
        return (ListArg<T>) listArg;
    }


    public <T extends Enum<T>> EnumArg<T> getEnumArg( int pos, Class<T> type ) {
        EnumArg<?> enumArg = getArg( pos, EnumArg.class );
        assert type.isInstance( enumArg.getArg() );
        return (EnumArg<T>) enumArg;
    }


    public <T extends Enum<T>> EnumArg<T> getEnumArg( String name, Class<T> type ) {
        EnumArg<?> enumArg = getArg( name, EnumArg.class );
        assert type.isInstance( enumArg.getArg() );
        return (EnumArg<T>) enumArg;
    }


    private <T extends PolyAlgArg> T getArg( Parameter p, Class<T> type ) {
        PolyAlgArg arg = getArg( p );
        assert type.isInstance( arg );
        return type.cast( arg );
    }


}
