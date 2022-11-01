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
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;

import java.util.*;

/**
 * Contains information for prepared queries, and also methods to handle the information
 */
@Slf4j
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
    private PGInterfaceErrorHandler errorHandler;


    /**
     * Creates the message itself
     * @param name name of the prepared statement
     * @param wholePrepareString the string which contains the prepared statement
     * @param ctx channelHandlerContext from the current connection
     */
    public PGInterfacePreparedMessage(String name, String wholePrepareString, ChannelHandlerContext ctx) {
        this.name = name;
        this.wholePrepareString = wholePrepareString;
        this.ctx = ctx;
    }

    /**
     * Creates the message itself
     * @param name name of the prepared statement
     * @param ctx channelHandlerContext from the current connection
     */
    public PGInterfacePreparedMessage(String name, ChannelHandlerContext ctx) {
        this.name = name;
        this.ctx = ctx;
    }

    /**
     * Sets the query itself (without prepare etc)
     * @param query The "pure" query
     */
    public void setQuery(String query) {
        this.query = query;
    }

    /**
     * the "whole" execute query
     * @param executeString execute query
     */
    public void setExecuteString(String executeString) {
        this.executeString = executeString;
    }

    /**
     * the "whole" prepare query
     * @param wholePrepareString prepare query
     */
    public void setWholePrepareString(String wholePrepareString) {
        this.wholePrepareString = wholePrepareString;
    }

    /**
     * the data types from the prepare query
     * @param dataTypes a list if all types (in the right order) from the prepare query
     */
    private void setDataTypes(List<String> dataTypes) { this.dataTypes = dataTypes; }

    /**
     * The values that will be inserted into the prepare query
     * @param data the values that will be inserted into the prepare query as string (since they arrive as string from the connection)
     */
    private void setData(List<String> data) { this.data = data; }

    /**
     * The delimiter that seperates the values in the execute query
     * @return a string sequence which is the delimiter in the execute query
     */
    public static String getExecuteDelimiter() {return executeDelimiter;}

    /**
     * The "pure" query (without prepare etc.)
     * @return the query itself (without prepare etc.)
     */
    public String getQuery() { return query;}

    public List<String> getDataTypes() { return dataTypes;}

    /**
     * Gets the values from the execute query
     * @return values from the execute query as string
     */
    public List<String> getData() { return data;}

    /**
     * From the execute string it extracts the values that will be inserted into the prepare query, sets these values in the message
     */
    public void extractAndSetValues() {
        //us execute string - seperator: ', '
        //bool ersetze...
        //cut string at ( and ) (remove chlammere) --> denn mach eif. split, ond problem solved...
        String onlyExecuteValues = executeString.split("\\(|\\)")[1];
        List<String> valueList = Arrays.asList(onlyExecuteValues.split(getExecuteDelimiter()));
        setData(valueList);
    }

    /**
     * From the prepare string it extracts the data types for the values to be inserted, sets these in the message
     */
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

    /**
     * in the prepared query it changes the parameter symbol from $ (from postgres) to ? (for polypheny), sets it in this message
     */
    public void changeParameterSymbol() {

        String[] parts = wholePrepareString.split("\\$");
        String newPrepareString = new String();

        for (int i = 1; i<parts.length; i++) {
            newPrepareString = newPrepareString + "?" + parts[i].substring(1);
        }

        setWholePrepareString(parts[0] + newPrepareString);

    }

    /**
     * extracts the "pure" query from the prepared string (so the query itself without anything else), sets it in this message
     */
    public void extractAndSetQuery() {
        String query = wholePrepareString.split("AS ")[1];
        setQuery(query);
    }

    /**
     * executes steps to prepare a query to be processed by polypheny
     * steps include: extract the types from the prepared query, changes the parameter symbol from $ to ?, extracts the query itself
     */
    public void prepareQuery() {
        changeParameterSymbol();
        extractAndSetTypes();
        extractAndSetQuery();
    }

    /**
     * Sets the parameter values (the datatype and the value) to the data context, so polypheny can process the prepared query
     * @param statement the statement that was created from the query
     */
    public void transformDataAndAddParameterValues (Statement statement) {
        long idx = 0;
        //TODO(FF): It doesn't work yet to insert several values (query below) --> tried 2 variants below, but both give same error (see bolow below)
        // (PREPARE lol (int, text, int) AS INSERT INTO pginterfacetesttable VALUES ($1, $2, $3), ($4, $5, $6); EXECUTE lol (4, 'HALLO', 4, 5, 'x', 5);
        //java.lang.RuntimeException: While executing SQL [INSERT INTO "PUBLIC"."tab5_part1004" ("_EXPR$0", "_EXPR$1", "_EXPR$2")
        //SELECT CAST(? AS INTEGER), CAST(? AS VARCHAR(255)), CAST(? AS INTEGER)
        //FROM (VALUES  (0)) AS "t" ("ZERO")
        //UNION ALL
        //SELECT CAST(? AS INTEGER), CAST(? AS VARCHAR(255)), CAST(? AS INTEGER)
        //FROM (VALUES  (0)) AS "t" ("ZERO")] on JDBC sub-schema

        //if o is as long as the number of data types
        for (String type : dataTypes) {
            List<Object> o = new ArrayList<>();
            for (int i = 0; i<data.size(); i++) {
                if (i%dataTypes.size() == (int) idx) {
                    String value = data.get(i);
                    o.add(transformData(value, type));
                }
            }
            AlgDataType algDataType = transformToAlgDataType(type, statement);
            statement.getDataContext().addParameterValues(idx, algDataType, o);
            idx ++;
        }

        /*
        // if o is as long as all the number of data
        for (int i = 0; i<data.size(); i++) {
            List<Object> o = new ArrayList<>();
            String value = data.get((int) idx);
            String type = dataTypes.get(i%dataTypes.size());
            o.add(transformData(value, type));
            AlgDataType algDataType = transformToAlgDataType(type, statement);
            statement.getDataContext().addParameterValues(idx, algDataType, o);
            idx ++;
        }

         */

    }

    /**
     * Transforms the data from a string into the correct format
     * @param value the value that needs to be transformed
     * @param type the type the value needs to be transformed into
     * @return returns the transformed value as an object
     */
    private Object transformData(String value, String type) {
        Object o = new Object();
        switch (type) {
            //TODO(FF): implement more data types
            case "int":
                o = Integer.valueOf(value);
                break;
            case "text":
                String pureValue = value;
                if (value.charAt(0) == '\'') {
                    pureValue = value.substring(1);
                    if(value.charAt(value.length()-1) == '\'') {
                        pureValue = pureValue.substring(0, pureValue.length() - 1);
                    }
                }
                o =  pureValue;
                break;
            case "bool":
                o =  Boolean.parseBoolean(value);
                break;
            case "numeric":
                o =  Double.parseDouble(value);
                break;
            default:
                errorHandler.sendSimpleErrorMessage("data type from from the prepared query is not yet supported by the postgres interface (transform data)");
                break;
        }
        return o;
    }

    /**
     * creates a AlgDataType according to the corresponding data type
     * @param type the type you want a AlgDataType for (types are the pgTypes)
     * @param statement needed to create the AlgDataType
     * @return returns the corresponding AlgDataType to the input type
     */
    private AlgDataType transformToAlgDataType(String type, Statement statement) {
        AlgDataType result = null;
        switch (type) {
            //TODO(FF): implement more data types
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
            default:
                errorHandler.sendSimpleErrorMessage("data type from from the prepared query is not yet supported by the postgres interface (create AlgDataType)");
                break;
        }

        return result;
    }


}
