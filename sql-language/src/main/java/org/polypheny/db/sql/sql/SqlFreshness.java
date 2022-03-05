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

package org.polypheny.db.sql.sql;


import java.util.List;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.util.ImmutableNullableList;


public class SqlFreshness extends SqlCall {

    // Considered as the lower bound
    public SqlNode toleratedFreshness;
    public SqlIdentifier evaluationType;
    public SqlIdentifier unit;


    public SqlFreshness(
            ParserPos pos,
            SqlNode toleratedFreshness,
            SqlIdentifier evaluationType,
            SqlIdentifier unit ) {
        super( pos );

        this.toleratedFreshness = toleratedFreshness;
        this.evaluationType = evaluationType;
        this.unit = unit;

    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( toleratedFreshness, evaluationType, unit );
    }


    @Override
    public Operator getOperator() {
        return null;
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( toleratedFreshness, evaluationType, unit );
    }
}
