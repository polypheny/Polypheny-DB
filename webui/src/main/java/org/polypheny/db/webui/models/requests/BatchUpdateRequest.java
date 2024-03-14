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

package org.polypheny.db.webui.models.requests;


import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.webui.Crud;


@Slf4j
public class BatchUpdateRequest {

    public String tableId;
    public final ArrayList<Update> updates = new ArrayList<>();


    public static class Update {

        final Map<String, String> oldPkValues = new HashMap<>();
        final Map<String, Value> newValues = new HashMap<>();
        long counter = 0;


        public String getQuery( String tableId, Statement statement, HttpServletRequest httpRequest ) throws IOException, ServletException {
            StringBuilder sBuilder = new StringBuilder();
            StringJoiner setClauses = new StringJoiner( "," );
            sBuilder.append( "UPDATE " ).append( tableId ).append( " SET " );
            for ( Entry<String, Value> entry : newValues.entrySet() ) {
                String fileName = entry.getValue().fileName;
                String value = entry.getValue().value;
                Catalog catalog = Catalog.getInstance();
                String[] split = tableId.split( "\\." );
                LogicalColumn logicalColumn;
                LogicalTable table = catalog.getSnapshot().rel().getTable( split[0], split[1] ).orElseThrow();
                logicalColumn = catalog.getSnapshot().rel().getColumn( table.id, entry.getKey() ).orElseThrow();
                if ( fileName == null && value == null ) {
                    setClauses.add( String.format( "\"%s\"=NULL", entry.getKey() ) );
                } else if ( value != null && fileName == null ) {
                    setClauses.add( String.format( "\"%s\"=%s", entry.getKey(), Crud.uiValueToSql( value, logicalColumn.type, logicalColumn.collectionsType ) ) );
                } else if ( value == null ) {// && fileName != null
                    if ( logicalColumn.type.getFamily() == PolyTypeFamily.MULTIMEDIA ) {
                        setClauses.add( String.format( "\"%s\"=?", entry.getKey() ) );
                        statement.getDataContext().addParameterValues( counter++, logicalColumn.getAlgDataType( AlgDataTypeFactory.DEFAULT ), ImmutableList.of( PolyBlob.of( httpRequest.getPart( fileName ).getInputStream() ) ) );
                    } else {
                        String data = IOUtils.toString( httpRequest.getPart( fileName ).getInputStream(), StandardCharsets.UTF_8 );
                        setClauses.add( String.format( "\"%s\"=%s", entry.getKey(), Crud.uiValueToSql( data, logicalColumn.type, logicalColumn.collectionsType ) ) );
                    }
                } else {
                    log.warn( "This should not happen" );
                }
            }
            sBuilder.append( setClauses.toString() );
            StringJoiner whereClauses = new StringJoiner( " AND " );
            for ( Entry<String, String> entry : oldPkValues.entrySet() ) {
                whereClauses.add( String.format( "\"%s\"='%s'", entry.getKey(), entry.getValue() ) );
            }
            sBuilder.append( " WHERE " ).append( whereClauses.toString() );
            return sBuilder.toString();
        }

    }


    private static class Value {

        String value;
        String fileName;

    }

}
