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

package org.polypheny.db.prepare;


import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlInsert;
import org.polypheny.db.sql.SqlOperatorTable;
import org.polypheny.db.sql.validate.SqlConformance;
import org.polypheny.db.sql.validate.SqlValidatorImpl;


/**
 * Validator.
 */
public class PolyphenyDbSqlValidator extends SqlValidatorImpl {

    public PolyphenyDbSqlValidator( SqlOperatorTable opTab, PolyphenyDbCatalogReader catalogReader, JavaTypeFactory typeFactory, SqlConformance conformance ) {
        super( opTab, catalogReader, typeFactory, conformance );
    }


    @Override
    protected RelDataType getLogicalSourceRowType( RelDataType sourceRowType, SqlInsert insert ) {
        final RelDataType superType = super.getLogicalSourceRowType( sourceRowType, insert );
        return ((JavaTypeFactory) typeFactory).toSql( superType );
    }


    @Override
    protected RelDataType getLogicalTargetRowType( RelDataType targetRowType, SqlInsert insert ) {
        final RelDataType superType = super.getLogicalTargetRowType( targetRowType, insert );
        return ((JavaTypeFactory) typeFactory).toSql( superType );
    }
}
