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

package org.polypheny.db.sql.validate;


import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelDataTypeFieldImpl;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.rel.type.RelRecordType;
import org.polypheny.db.sql.SqlBasicCall;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlSelect;
import org.polypheny.db.type.BasicPolyType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;


/**
 * Namespace offered by a sub-query.
 *
 * @see SelectScope
 * @see SetopNamespace
 */
public class SelectNamespace extends AbstractNamespace {

    private final SqlSelect select;


    /**
     * Creates a SelectNamespace.
     *
     * @param validator Validate
     * @param select Select node
     * @param enclosingNode Enclosing node
     */
    public SelectNamespace( SqlValidatorImpl validator, SqlSelect select, SqlNode enclosingNode ) {
        super( validator, enclosingNode );
        this.select = select;
    }


    // implement SqlValidatorNamespace, overriding return type
    @Override
    public SqlSelect getNode() {
        return select;
    }


    @Override
    public RelDataType validateImpl( RelDataType targetRowType ) {
        validator.validateSelect( select, targetRowType );
        // skewed test to generate dynamic rowtype as we need a correctly ordered rowtype with the dynamic columns TODO DL change
        if ( rowType.getFieldList().stream().anyMatch( field -> field.getName().equals( "_hidden" ) ) ) {
            List<RelDataTypeField> cheatTypes = new ArrayList<>( rowType.getFieldList() );
            SqlIdentifier field = (SqlIdentifier) ((SqlBasicCall) select.getWhere()).getOperands()[0];
            int i = cheatTypes.size();
            String name = field.names.size() == 2 ? field.names.get( 1 ) : field.names.get( 0 );

            if ( rowType.getFieldList().stream().noneMatch( f -> f.getName().equals( name ) ) ) {
                cheatTypes.add( new RelDataTypeFieldImpl( name, i, new BasicPolyType( RelDataTypeSystem.DEFAULT, PolyType.JSON, 300 ) ) );
            }

            return new RelRecordType( cheatTypes );
        }

        return rowType;
    }


    @Override
    public boolean supportsModality( SqlModality modality ) {
        return validator.validateModality( select, modality, false );
    }


    @Override
    public SqlMonotonicity getMonotonicity( String columnName ) {
        final RelDataType rowType = this.getRowTypeSansSystemColumns();
        final int field = PolyTypeUtil.findField( rowType, columnName );
        final SqlNode selectItem = validator.getRawSelectScope( select ).getExpandedSelectList().get( field );
        return validator.getSelectScope( select ).getMonotonicity( selectItem );
    }
}

