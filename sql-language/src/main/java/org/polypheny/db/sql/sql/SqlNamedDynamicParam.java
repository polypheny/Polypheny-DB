/*
 * Copyright 2019-2021 The Polypheny Project
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


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.DynamicParam;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.sql.sql.validate.SqlValidator;
import org.polypheny.db.sql.sql.validate.SqlValidatorScope;
import org.polypheny.db.util.Litmus;


/**
 * A <code>SqlDynamicParam</code> represents a dynamic parameter marker in an SQL statement. The textual order in which dynamic parameters appear within an SQL statement is the only property which distinguishes them, so this 0-based index is recorded as soon as the parameter is encountered.
 */
public class SqlNamedDynamicParam extends SqlDynamicParam {


    private final String name;


    public SqlNamedDynamicParam(int index, ParserPos pos, String name ) {
        super( index, pos );
        this.name = name;
    }

    public String getName() {
        return name;
    }

        //TODO(Nic): Add new Enum NAMED_DYNAMIC_PARAM?
//    @Override
//    public Kind getKind() {
//        return Kind.DYNAMIC_PARAM;
//    }

    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.print( name + "=>?" );
        writer.setNeedWhitespace( true );
    }

    @Override
    public boolean equalsDeep( Node node, Litmus litmus ) {
        if ( !(node instanceof SqlNamedDynamicParam) ) {
            return litmus.fail( "{} != {}", this, node );
        }
        SqlNamedDynamicParam that = (SqlNamedDynamicParam) node;
        if ( getIndex() != that.getIndex() ) {
            return litmus.fail( "{} != {}", this, node );
        }
        if ( !name.equals(that.getName()) ) {
            return litmus.fail( "{} != {}", this, node );
        }
        return litmus.succeed();
    }

}
