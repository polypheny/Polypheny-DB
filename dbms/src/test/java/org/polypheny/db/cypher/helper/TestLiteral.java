/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.cypher.helper;

import javax.annotation.Nullable;

public class TestLiteral implements TestObject {

    public final String value;


    public TestLiteral( String value ) {
        this.value = value;
    }


    @Override
    public boolean matches( Object other, boolean exclusive ) {
        if ( value == null && other == null ) {
            return true;
        }
        if ( value == null || other == null ) {
            return false;
        }

        return value.equals( other );
    }


    @Override
    public Object toPoly( String val ) {
        return val;
    }


    public static TestLiteral from( @Nullable Object value ) {
        return new TestLiteral( value != null ? value.toString() : null );
    }

}
