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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ViewWriterOrchestrator {
    private static final Pattern NAMED_ARGUMENTS_PATTERN = Pattern.compile("@(\\w+)=");
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
            String replaced = insertParameters(statement.getDataContext(), unquoted);
            // TODO(nic): Run query processor based on query type
            sqlProcessor.runSql(replaced, statement);
        }
    }

    private String insertParameters(DataContext dataContext, String query) {
        List<String> namedArguments = parseNamedArguments(query);
        return parameterize(namedArguments, query, dataContext);
    }

    private List<String> parseNamedArguments(String query) {
        List<String> namedArguments = new ArrayList<>();
        final Matcher matcher = NAMED_ARGUMENTS_PATTERN.matcher(query);
        while (matcher.find()) {
            String argument = matcher.group(0);
            namedArguments.add(argument);
        }
        return namedArguments;
    }

    private String parameterize(List<String> arguments, String query, DataContext dataContext) {
        String parameterizedQuery = query;
        Map<Long, Object> dataContextValues = dataContext.getParameterValues().get(0);
        for(int index = 1; index <= arguments.size(); index++) {
            Object parameterValue = dataContextValues.get(Long.valueOf(index)); // boxing for map keys access
            String parameter = arguments.get(index - 1); // dataContext is 1-based
            parameterizedQuery = replaceParameter(parameterizedQuery, parameter, parameterValue);
        }
        return parameterizedQuery;
    }

    private String replaceParameter(String query, String parameter, Object parameterValue) {
        String replacedParameter = parameter + parameterValue.toString();
        return query.replace(parameter, replacedParameter);
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
