/*
 * Copyright 2019-2020 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.sql;


import org.polypheny.db.core.ParserPos;
import org.polypheny.db.sql.util.SqlVisitor;
import org.polypheny.db.sql.validate.SqlMonotonicity;
import org.polypheny.db.sql.validate.SqlValidator;
import org.polypheny.db.sql.validate.SqlValidatorScope;
import org.polypheny.db.util.Litmus;


/**
 * A <code>SqlDynamicParam</code> represents a dynamic parameter marker in an SQL statement. The textual order in which dynamic parameters appear within an SQL statement is the only property which distinguishes them, so this 0-based index is recorded as soon as the parameter is encountered.
 */
public class SqlDynamicParam extends SqlNode {

    private final int index;


    public SqlDynamicParam( int index, ParserPos pos ) {
        super( pos );
        this.index = index;
    }


    @Override
    public SqlNode clone( ParserPos pos ) {
        return new SqlDynamicParam( index, pos );
    }


    @Override
    public SqlKind getKind() {
        return SqlKind.DYNAMIC_PARAM;
    }


    public int getIndex() {
        return index;
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.dynamicParam( index );
    }


    @Override
    public void validate( SqlValidator validator, SqlValidatorScope scope ) {
        validator.validateDynamicParam( this );
    }


    @Override
    public SqlMonotonicity getMonotonicity( SqlValidatorScope scope ) {
        return SqlMonotonicity.CONSTANT;
    }


    @Override
    public <R> R accept( SqlVisitor<R> visitor ) {
        return visitor.visit( this );
    }


    @Override
    public boolean equalsDeep( SqlNode node, Litmus litmus ) {
        if ( !(node instanceof SqlDynamicParam) ) {
            return litmus.fail( "{} != {}", this, node );
        }
        SqlDynamicParam that = (SqlDynamicParam) node;
        if ( this.index != that.index ) {
            return litmus.fail( "{} != {}", this, node );
        }
        return litmus.succeed();
    }
}
