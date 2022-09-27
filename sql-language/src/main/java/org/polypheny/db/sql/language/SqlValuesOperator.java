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

package org.polypheny.db.sql.language;


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.ValuesOperator;

/**
 * The <code>VALUES</code> operator.
 */
public class SqlValuesOperator extends SqlSpecialOperator implements ValuesOperator {

    public SqlValuesOperator() {
        super( "VALUES", Kind.VALUES );
    }


    @Override
    public void unparse( SqlWriter writer, SqlCall call, int leftPrec, int rightPrec ) {
        final SqlWriter.Frame frame = writer.startList( SqlWriter.FrameTypeEnum.VALUES, "VALUES", "" );
        for ( Node operand : call.getOperandList() ) {
            writer.sep( "," );
            ((SqlNode) operand).unparse( writer, 0, 0 );
        }
        writer.endList( frame );
    }

}

