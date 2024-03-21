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
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;

public class ListArg<E extends PolyAlgArg> implements PolyAlgArg {

    private final List<E> args;
    private final List<String> aliases;
    private final AlgNode algNode;
    private final boolean unpackValues;


    public ListArg( List<E> args, List<String> aliases, AlgNode fieldNameProvider, boolean unpackValues ) {
        this.args = args;
        this.aliases = aliases;
        this.algNode = fieldNameProvider;
        this.unpackValues = unpackValues;
    }


    public <T> ListArg( List<T> rawArgs, Function<T, E> converter ) {
        this( rawArgs, converter, null, null, false );
    }


    public <T> ListArg( List<T> rawArgs, Function<T, E> converter, boolean unpackValues ) {
        this( rawArgs, converter, null, null, unpackValues );
    }


    public <T> ListArg( List<T> rawArgs, Function<T, E> converter, AlgNode fieldNameProvider ) {
        this( rawArgs, converter, null, fieldNameProvider, false );
    }


    public <T> ListArg( List<T> rawArgs, Function<T, E> converter, AlgNode fieldNameProvider, boolean unpackValues ) {
        this( rawArgs, converter, null, fieldNameProvider, unpackValues );
    }


    public <T> ListArg( List<T> rawArgs, Function<T, E> converter, List<String> aliases, boolean unpackValues ) {
        this( rawArgs, converter, aliases, null, unpackValues );
    }


    public <T> ListArg( List<T> rawArgs, Function<T, E> converter, List<String> aliases, AlgNode fieldNameProvider ) {
        this( rawArgs.stream().map( converter ).toList(), aliases, fieldNameProvider, false );
    }


    public <T> ListArg( List<T> rawArgs, Function<T, E> converter, List<String> aliases, AlgNode fieldNameProvider, boolean unpackValues ) {
        this( rawArgs.stream().map( converter ).toList(), aliases, fieldNameProvider, unpackValues );
    }


    @Override
    public ParamType getType() {
        if ( args.isEmpty() ) {
            return ParamType.EMPTY_LIST;
        }
        return args.get( 0 ).getType();
    }


    @Override
    public String toPolyAlg() {
        List<String> strArgs = args.stream().map( PolyAlgArg::toPolyAlg ).toList();
        if ( algNode != null ) {
            strArgs = PolyAlgUtils.replaceWithFieldNames( algNode, strArgs );
        }
        if ( aliases != null ) {
            strArgs = PolyAlgUtils.appendAliases( strArgs, aliases );
        }
        return PolyAlgUtils.joinMultiValued( strArgs, unpackValues );
    }

}
