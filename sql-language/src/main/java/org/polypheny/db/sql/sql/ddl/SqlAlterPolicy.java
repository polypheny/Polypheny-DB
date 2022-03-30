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

package org.polypheny.db.sql.sql.ddl;

import java.util.List;
import org.polypheny.db.adaptiveness.models.PolicyChangeRequest;
import org.polypheny.db.adaptiveness.policy.PoliceUtil.ClauseName;
import org.polypheny.db.adaptiveness.policy.PoliciesManager;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.sql.sql.SqlAlter;
import org.polypheny.db.sql.sql.SqlNode;
import org.polypheny.db.sql.sql.SqlOperator;
import org.polypheny.db.sql.sql.SqlSpecialOperator;
import org.polypheny.db.sql.sql.SqlWriter;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;

public class SqlAlterPolicy extends SqlAlter {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "ALTER POLICY", Kind.OTHER_DDL );

    private final SqlNode key;
    private final SqlNode value;


    public SqlAlterPolicy( ParserPos pos, SqlNode key, SqlNode value ) {
        super( OPERATOR, pos );
        this.key = key;
        this.value = value;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( key, value );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( key, value );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "POLICY" );
        key.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "SET" );
        value.unparse( writer, leftPrec, rightPrec );
    }


    /*
    ALTER POLICY is only possible for the whole of Polypheny, it is not possible to only select namespace, entity
     */
    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        String keyStr = key.toString();
        String valueStr = value.toString();

        PoliciesManager policiesManager = PoliciesManager.getInstance();

        ClauseName clauseName = ClauseName.getClauseName( keyStr );

        if ( clauseName != null ) {
            PolicyChangeRequest policyChangeRequest;
            switch ( clauseName.getClauseType() ) {
                case BOOLEAN:
                    policyChangeRequest = new PolicyChangeRequest( "BooleanChangeRequest", clauseName.name(), "POLYPHENY", Boolean.parseBoolean( valueStr ), -1L );
                    break;
                case NUMBER:
                    policyChangeRequest = new PolicyChangeRequest( "NumberChangeRequest", clauseName.name(), "POLYPHENY", Integer.parseInt( valueStr ), -1L );
                    break;
                default:
                    throw new RuntimeException( "Please make sure to use a valid Clause Name." );
            }

            policiesManager.addClause( policyChangeRequest );
            policiesManager.updateClauses( policyChangeRequest );

        }


    }

}
