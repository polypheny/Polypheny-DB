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

package org.polypheny.db.sql.language.fun;


import com.google.common.base.Preconditions;
import java.util.Objects;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.nodes.QuantifyOperator;


/**
 * Definition of the SQL <code>ALL</code> and <code>SOME</code>operators.
 *
 * Each is used in combination with a relational operator:
 * <code>&lt;</code>, <code>&le;</code>,
 * <code>&gt;</code>, <code>&ge;</code>,
 * <code>=</code>, <code>&lt;&gt;</code>.
 *
 * <code>ANY</code> is a synonym for <code>SOME</code>.
 */
public class SqlQuantifyOperator extends SqlInOperator implements QuantifyOperator {

    @Getter
    public final Kind comparisonKind;


    /**
     * Creates a SqlQuantifyOperator.
     *
     * @param kind Either ALL or SOME
     * @param comparisonKind Either <code>&lt;</code>, <code>&le;</code>, <code>&gt;</code>, <code>&ge;</code>, <code>=</code> or <code>&lt;&gt;</code>.
     */
    public SqlQuantifyOperator( Kind kind, Kind comparisonKind ) {
        super( comparisonKind.sql + " " + kind, kind );
        this.comparisonKind = Objects.requireNonNull( comparisonKind );
        Preconditions.checkArgument( comparisonKind == Kind.EQUALS
                || comparisonKind == Kind.NOT_EQUALS
                || comparisonKind == Kind.LESS_THAN_OR_EQUAL
                || comparisonKind == Kind.LESS_THAN
                || comparisonKind == Kind.GREATER_THAN_OR_EQUAL
                || comparisonKind == Kind.GREATER_THAN );
        Preconditions.checkArgument( kind == Kind.SOME
                || kind == Kind.ALL );
    }

}

