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

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelect;


/**
 * Represents the name-resolution context for expressions in an GROUP BY clause.
 *
 * In some dialects of SQL, the GROUP BY clause can reference column aliases in the SELECT clause. For example, the query
 *
 * <blockquote><code>
 * SELECT empno AS x<br>
 * FROM emp<br>
 * GROUP BY x
 * </code></blockquote>
 *
 * is valid.
 */
public class GroupByScope extends DelegatingScope {

    private final SqlNodeList groupByList;
    private final SqlSelect select;


    GroupByScope( SqlValidatorScope parent, SqlNodeList groupByList, SqlSelect select ) {
        super( parent );
        this.groupByList = groupByList;
        this.select = select;
    }


    @Override
    public SqlNode getNode() {
        return groupByList;
    }


    @Override
    public void validateExpr( SqlNode expr ) {
        SqlNode expanded = validator.expandGroupByOrHavingExpr( expr, this, select, false );

        // expression needs to be valid in parent scope too
        parent.validateExpr( expanded );
    }
}

