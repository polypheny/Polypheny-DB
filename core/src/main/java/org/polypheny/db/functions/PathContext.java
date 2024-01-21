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

package org.polypheny.db.functions;

import org.apache.calcite.linq4j.tree.Expression;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.functions.Functions.PathMode;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Static;

/**
 * Returned path context of JsonApiCommonSyntax, public for testing.
 */
public class PathContext extends PolyValue {

    public final PathMode mode;
    public final PolyValue pathReturned;
    public final Exception exc;


    private PathContext( PathMode mode, PolyValue pathReturned, Exception exc ) {
        super( PolyType.JSON );
        this.mode = mode;
        this.pathReturned = pathReturned;
        this.exc = exc;
    }


    public static PathContext withUnknownException( Exception exc ) {
        return new PathContext( PathMode.UNKNOWN, null, exc );
    }


    public static PathContext withStrictException( Exception exc ) {
        return new PathContext( PathMode.STRICT, null, exc );
    }


    public static PathContext withReturned( PathMode mode, PolyValue pathReturned ) {
        if ( mode == PathMode.UNKNOWN ) {
            throw Static.RESOURCE.illegalJsonPathMode( mode.toString() ).ex();
        }
        if ( mode == PathMode.STRICT && pathReturned == null ) {
            throw Static.RESOURCE.strictPathModeRequiresNonEmptyValue().ex();
        }
        return new PathContext( mode, pathReturned, null );
    }


    @Override
    public String toString() {
        return "PathContext{"
                + "mode=" + mode
                + ", pathReturned=" + pathReturned
                + ", exc=" + exc
                + '}';
    }


    @Override
    public @Nullable Long deriveByteSize() {
        throw new RuntimeException( "Not implemented" );
    }


    @Override
    public Object toJava() {
        return this;
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        throw new RuntimeException( "Not implemented" );
    }


    @Override
    public Expression asExpression() {
        throw new RuntimeException( "Not implemented" );
    }


    @Override
    public PolySerializable copy() {
        throw new RuntimeException( "Not implemented" );
    }

}
