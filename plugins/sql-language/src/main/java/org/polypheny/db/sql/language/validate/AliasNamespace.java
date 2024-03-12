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

package org.polypheny.db.sql.language.validate;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.util.Util;


/**
 * Namespace for an <code>AS t(c1, c2, ...)</code> clause.
 *
 * A namespace is necessary only if there is a column list, in order to re-map column names; a <code>relation AS t</code> clause just uses the same namespace as <code>relation</code>.
 */
public class AliasNamespace extends AbstractNamespace {

    protected final SqlCall call;


    /**
     * Creates an AliasNamespace.
     *
     * @param validator Validator
     * @param call Call to AS operator
     * @param enclosingNode Enclosing node
     */
    protected AliasNamespace( SqlValidatorImpl validator, SqlCall call, SqlNode enclosingNode ) {
        super( validator, enclosingNode );
        this.call = call;
        assert call.getOperator().getOperatorName() == OperatorName.AS;
    }


    @Override
    protected AlgDataType validateImpl( AlgDataType targetRowType ) {
        final List<String> nameList = new ArrayList<>();
        final List<SqlNode> operands = call.getSqlOperandList();
        final SqlValidatorNamespace childNs = validator.getSqlNamespace( operands.get( 0 ) );
        final AlgDataType rowType = childNs.getRowTypeSansSystemColumns();
        final List<SqlNode> columnNames = Util.skip( operands, 2 );
        for ( final SqlNode operand : columnNames ) {
            String name = ((SqlIdentifier) operand).getSimple();
            if ( nameList.contains( name ) ) {
                throw validator.newValidationError( operand, RESOURCE.aliasListDuplicate( name ) );
            }
            nameList.add( name );
        }
        if ( nameList.size() != rowType.getFieldCount() ) {
            // Position error over all column names
            final SqlNode node =
                    operands.size() == 3
                            ? operands.get( 2 )
                            : new SqlNodeList( columnNames, ParserPos.sum( columnNames ) );
            throw validator.newValidationError( node, RESOURCE.aliasListDegree( rowType.getFieldCount(), getString( rowType ), nameList.size() ) );
        }
        final List<AlgDataType> typeList = new ArrayList<>();
        for ( AlgDataTypeField field : rowType.getFields() ) {
            typeList.add( field.getType() );
        }
        return validator.getTypeFactory().createStructType( typeList.stream().map( t -> (Long) null ).toList(), typeList, nameList );
    }


    private String getString( AlgDataType rowType ) {
        StringBuilder buf = new StringBuilder();
        buf.append( "(" );
        for ( AlgDataTypeField field : rowType.getFields() ) {
            if ( field.getIndex() > 0 ) {
                buf.append( ", " );
            }
            buf.append( "'" );
            buf.append( field.getName() );
            buf.append( "'" );
        }
        buf.append( ")" );
        return buf.toString();
    }


    @Override
    public SqlNode getNode() {
        return call;
    }


    @Override
    public String translate( String name ) {
        final AlgDataType underlyingRowType = validator.getValidatedNodeType( call.operand( 0 ) );
        int i = 0;
        for ( AlgDataTypeField field : rowType.getFields() ) {
            if ( field.getName().equals( name ) ) {
                return underlyingRowType.getFields().get( i ).getName();
            }
            ++i;
        }
        throw new AssertionError( "unknown field '" + name + "' in rowtype " + underlyingRowType );
    }

}

