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

import java.util.List;
import java.util.function.Function;
import lombok.Getter;
import lombok.NonNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;

public class ListArg<E extends PolyAlgArg> implements PolyAlgArg {

    @Getter
    private final List<E> args;
    @Getter
    private final List<String> aliases;
    private final boolean unpackValues;


    public ListArg( List<E> args, List<String> aliases, boolean unpackValues ) {
        this.args = args;
        this.aliases = aliases;
        this.unpackValues = unpackValues;
    }


    public <T> ListArg( List<T> rawArgs, Function<T, E> converter ) {
        this( rawArgs, converter, null, false );
    }


    public <T> ListArg( List<T> rawArgs, Function<T, E> converter, boolean unpackValues ) {
        this( rawArgs, converter, null, unpackValues );
    }


    public <T> ListArg( List<T> rawArgs, Function<T, E> converter, List<String> aliases ) {
        this( rawArgs.stream().map( converter ).toList(), aliases, false );
    }


    public <T> ListArg( List<T> rawArgs, Function<T, E> converter, List<String> aliases, boolean unpackValues ) {
        this( rawArgs.stream().map( converter ).toList(), aliases, unpackValues );
    }


    @Override
    public ParamType getType() {
        if ( args.isEmpty() ) {
            return ParamType.EMPTY_LIST;
        }
        return args.get( 0 ).getType();
    }


    @Override
    public String toPolyAlg( AlgNode context, @NonNull List<String> inputFieldNames ) {
        List<String> strArgs = args.stream().map( a -> a.toPolyAlg( context, inputFieldNames ) ).toList();

        if ( aliases != null ) {
            strArgs = PolyAlgUtils.appendAliases( strArgs, aliases );
        }
        return PolyAlgUtils.joinMultiValued( strArgs, unpackValues );
    }

}
