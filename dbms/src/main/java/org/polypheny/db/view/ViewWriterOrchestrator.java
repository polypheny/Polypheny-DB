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

package org.polypheny.db.view;

import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.entity.CatalogTrigger;
import org.polypheny.db.processing.SqlProcessorFacade;
import org.polypheny.db.processing.SqlProcessorImpl;
import org.polypheny.db.processing.shuttles.QueryParameterizer;
import org.polypheny.db.transaction.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class ViewWriterOrchestrator {
    private final SqlProcessorFacade sqlProcessor = new SqlProcessorFacade(new SqlProcessorImpl());

    public void writeView(AlgNode node, Statement statement, CatalogTable catalogTable) {
        List<CatalogTrigger> triggers = Catalog.getInstance()
                .getTriggers(catalogTable.schemaId)
                // TODO(nic): Filter for event (insert, update, delete)
                .stream().filter(trigger -> trigger.getTableId() == catalogTable.id).collect(Collectors.toList());
        QueryParameterizer queryParameterizer = extractParameters(node);
        populateDatacontext(statement, queryParameterizer);
        for (CatalogTrigger trigger : triggers) {
            String unquoted = trigger.getQuery().substring(1, trigger.getQuery().length() - 2); // remove quotes and ;
            sqlProcessor.runSql(unquoted, statement);
        }
    }

    private QueryParameterizer extractParameters(AlgNode node) {
        // Duplicated code from AQP#parameterize
        List<AlgDataType> parameterRowTypeList = new ArrayList<>();
        node.getRowType().getFieldList().forEach( algDataTypeField -> parameterRowTypeList.add( algDataTypeField.getType() ) );
        QueryParameterizer queryParameterizer = new QueryParameterizer( node.getRowType().getFieldCount(), parameterRowTypeList );
        AlgNode parameterized = node.accept( queryParameterizer );
        List<AlgDataType> types = queryParameterizer.getTypes();
        return queryParameterizer;
    }

    private void populateDatacontext(Statement statement, QueryParameterizer queryParameterizer) {
        for ( List<DataContext.ParameterValue> values : queryParameterizer.getValues().values() ) {
            List<Object> o = new ArrayList<>();
            for ( DataContext.ParameterValue v : values ) {
                o.add( v.getValue() );
            }
            statement.getDataContext().addParameterValues( values.get( 0 ).getIndex(), values.get( 0 ).getType(), o );
        }
    }
}
