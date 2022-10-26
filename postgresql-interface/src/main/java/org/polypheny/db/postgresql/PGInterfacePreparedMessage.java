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

package org.polypheny.db.postgresql;

import io.netty.channel.ChannelHandlerContext;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;

import java.util.*;

public class PGInterfacePreparedMessage {
    private String name;
    private ChannelHandlerContext ctx;
    private String query;
    private String wholePrepareString;
    private String executeString;
    private List<String> dataTypes;
    private List<String> data;
    private static final String  executeDelimiter = ", ";
    private Map<Long, AlgDataType> typesPolyphey = new HashMap<Long, AlgDataType>();
    private List<Map<Long, Object>> valuesPolypeny = new ArrayList<Map<Long, Object>>();


    public PGInterfacePreparedMessage(String name, String wholePrepareString, ChannelHandlerContext ctx) {
        this.name = name;
        this.wholePrepareString = wholePrepareString;
        this.ctx = ctx;
    }

    public PGInterfacePreparedMessage(String name, ChannelHandlerContext ctx) {
        this.name = name;
        this.ctx = ctx;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public void setExecuteString(String executeString) {
        this.executeString = executeString;
    }

    public void setWholePrepareString(String wholePrepareString) {
        this.wholePrepareString = wholePrepareString;
    }

    private void setDataTypes(List<String> dataTypes) { this.dataTypes = dataTypes; }

    private void setData(List<String> data) { this.data = data; }

    public static String getExecuteDelimiter() {return executeDelimiter;}

    public String getQuery() { return query;}

    public List<String> getDataTypes() { return dataTypes;}

    public List<String> getData() { return data;}

    public void extractAndSetValues() {
        //us execute string - seperator: ', '
        //bool ersetze...
        //cut string at ( and ) (remove chlammere) --> denn mach eif. split, ond problem solved...
        String onlyExecuteValues = executeString.split("\\(|\\)")[1];
        List<String> valueList = Arrays.asList(onlyExecuteValues.split(getExecuteDelimiter()));
        setData(valueList);    //FIXME(FF): setData, ned setDataTypes
    }

    public void extractAndSetTypes() {
        String types = wholePrepareString.split("\\(|\\)")[1];
        List<String> typeList = Arrays.asList(types.split(getExecuteDelimiter()));

        //replace all bool with boolean to match polypheny dt
        if (typeList.contains("bool") || typeList.contains("BOOL")) {
            ListIterator<String> iterator = typeList.listIterator();
            while (iterator.hasNext()) {
                String next = iterator.next();
                if (next.equals("bool")) {
                    typeList.set(iterator.nextIndex()-1, "BOOLEAN");
                }
            }
        }
        setDataTypes(typeList);
    }

    public void changeParameterSymbol() {

        String[] parts = wholePrepareString.split("\\$");
        String newPrepareString = new String();

        for (int i = 1; i<parts.length; i++) {
            newPrepareString = newPrepareString + "?" + parts[i].substring(1);
        }

        setWholePrepareString(parts[0] + newPrepareString);

    }

    public void extractAndSetQuery() {
        String query = wholePrepareString.split("AS ")[1];
        setQuery(query);
    }

    public void prepareQuery() {
        extractAndSetTypes();
        changeParameterSymbol();
        extractAndSetTypes();
        extractAndSetQuery();
    }

    public void transformDataAndAddParameterValues (Statement statement) {
        long idx = 0;
        for (String type : dataTypes) {
            List<Object> o = new ArrayList<>();
            for (String value : data) {
                o.add(transformData(value, type));
            }
            AlgDataType algDataType = transformToAlgDataType(type, statement);
            statement.getDataContext().addParameterValues(idx, algDataType, o);
        }
        /*
        // Add values to data context
        //values() chonnt vo map
        Map<Integer, List<DataContext.ParameterValue>> values = null;

        for(int i=0; i<values.size(); i++) {
            List<DataContext.ParameterValue>valFor = values.get(i);
            List<Object> o = new ArrayList<>();
            for(int j=0; j<valFor.size(); j++) {
                DataContext.ParameterValue v = valFor.get(j);
                o.add(v.getValue()); // -->the object, which is the value
            }
            //addParameterValues( long index, AlgDataType type, List<Object> data );
            statement.getDataContext().addParameterValues(valFor.get(0).getIndex(), valFor.get(0).getType(), o);
        }


        for ( List<DataContext.ParameterValue> values : queryParameterizer.getValues().values() ) {
            List<Object> o = new ArrayList<>();
            for ( DataContext.ParameterValue v : values ) {
                o.add( v.getValue() );
            }
            statement.getDataContext().addParameterValues( values.get( 0 ).getIndex(), values.get( 0 ).getType(), o );
        }

         */
    }

    private Object transformData(String value, String type) {
        Object o = new Object();
        switch (type) {
            case "int":
                o = Integer.valueOf(value);
                break;
            case "text":
                o =  value;
                break;
            case "bool":
                o =  Boolean.parseBoolean(value);
                break;
            case "numeric":
                o =  Double.parseDouble(value);
                break;
        }
        return o;
    }

    private AlgDataType transformToAlgDataType(String type, Statement statement) {
        AlgDataType result = null;
        switch (type) {
            case "int":
                result = statement.getTransaction().getTypeFactory().createPolyType(PolyType.INTEGER);
                break;
            case "text":
                result = statement.getTransaction().getTypeFactory().createPolyType(PolyType.VARCHAR, 255); //TODO(FF): how do I know the precision?
                break;
            case "bool":
                result = statement.getTransaction().getTypeFactory().createPolyType(PolyType.BOOLEAN);
                break;
            case "numeric":
                result = statement.getTransaction().getTypeFactory().createPolyType(PolyType.DECIMAL, 3, 3);
                break;
        }

        return result;
    }


}
