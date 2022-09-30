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

package org.polypheny.db.exploreByExample;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import weka.classifiers.trees.J48;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instances;
import weka.core.Utils;


@Slf4j
public class Explore {

    @Getter
    private int id;
    @Setter
    List<List<String>> uniqueValues;
    @Getter
    @Setter
    private List<String[]> labeled;
    @Getter
    @Setter
    private String[] dataType;
    @Setter
    @Getter
    private String[] labels;
    private Instances unlabeledData;
    @Getter
    private String buildGraph;
    @Getter
    private String[][] data;
    @Getter
    private String query;
    @Getter
    private String sqlStatement;
    @Getter
    private boolean classificationPossible = true;
    private ExploreQueryProcessor exploreQueryProcessor;
    @Getter
    private List<String[]> dataAfterClassification;
    @Getter
    private int tableSize;
    private List<String> qualifiedNames = new ArrayList<>();
    @Getter
    private String classifiedSqlStatement;
    private Map<String, String> nameAndType = new HashMap<>();
    @Getter
    private boolean isConvertedToSql;
    @Getter
    private boolean includesJoin;
    public boolean isDataAfterClassification;


    public Explore( int identifier, String query, ExploreQueryProcessor exploreQueryProcessor ) {
        this.id = identifier;
        this.query = query;
        this.sqlStatement = query;
        this.exploreQueryProcessor = exploreQueryProcessor;
        this.dataType = getTypeInfo( query );
    }


    /**
     * First exploration/classification with the labeled entries from the user
     */
    public void exploreUserInput() {
        isDataAfterClassification = true;
        List<String[]> initialDataClassification = getAllData( sqlStatement );
        unlabeledData = createInstance( initialDataClassification, rotate2dArray( initialDataClassification ), dataType, uniqueValues );
        dataAfterClassification = classifyUnlabeledData( trainData( createInstance( rotate2dArray( labeled ), labeled, dataType, uniqueValues ) ), unlabeledData );
    }


    /**
     * For each additional exploration
     *
     * @param labeled data from user
     */
    public void updateExploration( List<String[]> labeled ) {
        dataAfterClassification = classifyUnlabeledData( trainData( createInstance( rotate2dArray( labeled ), labeled, dataType, uniqueValues ) ), unlabeledData );
    }


    /**
     * Final classification of all data according to isConvertedToSql with Weka or by creating a sql query
     *
     * @param labeled data from user
     * @param isConvertedToSql boolean if it should be converted to SQL or not
     */
    public void classifyAllData( List<String[]> labeled, boolean isConvertedToSql ) {
        List<String[]> allData = getAllData( this.query );
        isDataAfterClassification = false;
        classificationPossible = false;
        this.isConvertedToSql = isConvertedToSql;
        if ( isConvertedToSql ) {
            classifiedSqlStatement = sqlClassifiedData( trainData( createInstance( rotate2dArray( labeled ), labeled, dataType, uniqueValues ) ), nameAndType );
            tableSize = getSQLCount( classifiedSqlStatement );
        } else {
            unlabeledData = createInstance( allData, rotate2dArray( allData ), dataType, uniqueValues );
            data = classifyData( trainData( createInstance( rotate2dArray( labeled ), labeled, dataType, uniqueValues ) ), unlabeledData );
        }
    }


    /**
     * Executes query to get the type information about all the queries
     *
     * @param query from user interface
     * @return different data types of a query
     */

    public String[] getTypeInfo( String query ) {
        ExploreQueryResult exploreQueryResult = this.exploreQueryProcessor.executeSQL( query );

        String[] fullNames = query.split( "SELECT " )[1].split( "\nFROM" )[0].replace( " ", "" ).toLowerCase().split( "," );
        String[] dataType = new String[fullNames.length];

        for ( int i = 0; i < exploreQueryResult.typeInfo.size(); i++ ) {
            String name = fullNames[i].substring( fullNames[i].lastIndexOf( "." ) + 1 );
            name = name.replace( "\"", "" ); // remove quotes
            if ( exploreQueryResult.name.get( i ).startsWith( name ) ) {
                String type = exploreQueryResult.typeInfo.get( i );
                if ( Arrays.asList( "VARCHAR", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL", "JSON" ).contains( type ) ) {
                    dataType[i] = exploreQueryResult.typeInfo.get( i );
                    nameAndType.put( fullNames[i], dataType[i] );
                } else {
                    dataType = null;
                    return null;
                }
            }
        }

        Collections.addAll( qualifiedNames, fullNames );
        qualifiedNames.add( "classifications" );

        return dataType;
    }


    /**
     * Creates the SQL query for the initial table
     */
    public void createSQLStatement() {

        List<String> selectedCols = new ArrayList<>();
        List<String> whereClause = new ArrayList<>();
        List<String> allCols = new ArrayList<>();
        List<String> groupByList = new ArrayList<>();
        List<String> intCols = new ArrayList<>();
        List<String> unionList = new ArrayList<>();
        List<String> allBlankCols = new ArrayList<>();
        includesJoin = false;
        int rowCount = getSQLCount( sqlStatement + "\nLIMIT 60" );

        if ( rowCount < 10 ) {
            classificationPossible = false;
            tableSize = rowCount;
        } else if ( rowCount > 10 && rowCount < 40 ) {
            tableSize = rowCount;
        } else if ( rowCount > 40 ) {
            String selectDistinct = sqlStatement.replace( "SELECT", "SELECT DISTINCT" ) + "\nLIMIT 60";
            if ( sqlStatement.contains( "WHERE" ) ) {
                includesJoin = true;
                sqlStatement = selectDistinct;
                tableSize = getSQLCount( sqlStatement.replace( "\nLIMIT 60", "\nLIMIT 200" ) );
                return;
            }
            if ( sqlStatement.split( "\nFROM" )[1].split( "," ).length > 1 ) {
                sqlStatement = selectDistinct;
                tableSize = getSQLCount( sqlStatement );
                return;
            }
            rowCount = getSQLCount( selectDistinct );
            if ( rowCount < 10 ) {
                tableSize = rowCount;
                classificationPossible = false;
            } else if ( rowCount > 10 && rowCount < 40 ) {
                tableSize = rowCount;
                sqlStatement = selectDistinct;
            } else if ( rowCount > 40 ) {

                selectedCols = Arrays.asList( sqlStatement.replace( "SELECT", "" ).split( "\nFROM" )[0].split( "," ) );

                boolean includesInteger = false;
                boolean includesVarchar = false;
                for ( int i = 0; i < selectedCols.size(); i++ ) {
                    if ( dataType[i].equals( "VARCHAR" ) ) {
                        includesVarchar = true;
                        allCols.add( selectedCols.get( i ) );
                        groupByList.add( selectedCols.get( i ) );
                        allBlankCols.add( selectedCols.get( i ) );
                    }
                    if ( dataType[i].equals( "INTEGER" ) || dataType[i].equals( "BIGINT" ) || dataType[i].equals( "SMALLINT" ) || dataType[i].equals( "TINYINT" ) || dataType[i].equals( "DECIMAL" ) ) {
                        includesInteger = true;
                        allCols.add( "AVG(" + selectedCols.get( i ) + ") AS " + selectedCols.get( i ).substring( selectedCols.get( i ).lastIndexOf( '.' ) + 1 ) );
                        intCols.add( selectedCols.get( i ) );
                        allBlankCols.add( selectedCols.get( i ) );
                    }
                }

                if ( includesInteger ) {
                    getMins( intCols ).forEach( ( s, min ) -> unionList.add( "\nUNION \n(SELECT " + String.join( ",", allBlankCols ) + "\nFROM" + sqlStatement.split( "\nFROM" )[1] + "\nWHERE " + s + "=" + min + "\nLIMIT 1)" ) );
                    getMaxs( intCols ).forEach( ( s, max ) -> unionList.add( "\nUNION \n(SELECT " + String.join( ",", allBlankCols ) + "\nFROM" + sqlStatement.split( "\nFROM" )[1] + "\nWHERE " + s + "=" + max + "\nLIMIT 1)" ) );
                }

                if ( includesVarchar ) {
                    sqlStatement = "(SELECT " + String.join( ",", allCols ) + "\nFROM" + sqlStatement.split( "\nFROM" )[1] + "\nGROUP BY " + String.join( ",", groupByList ) + "\nLIMIT 200)" + String.join( "", unionList );
                } else {
                    sqlStatement = "SELECT " + String.join( ",", allCols ) + "\nFROM" + sqlStatement.split( "\nFROM" )[1] + String.join( "", unionList );
                }

                tableSize = getSQLCount( sqlStatement + "\nLIMIT 200" );
            }
        }
    }


    /**
     * Gets all unique Values for a given query and adds true and false for Weka Instances
     */
    public List<List<String>> getStatistics( String query ) {

        List<List<String>> uniqueValues = new ArrayList<>();
        List<ExploreQueryResult> values = this.exploreQueryProcessor.getAllUniqueValues( prepareColInfo( query ), query );

        for ( ExploreQueryResult uniqueValue : values ) {
            List<String> data = new ArrayList<>();
            for ( int i = 0; i < uniqueValue.data.length; i++ ) {
                List<String> cols = new ArrayList<>();
                for ( int j = 0; j < uniqueValue.data[i].length; j++ ) {
                    cols.add( uniqueValue.data[i][j] );
                }
                data.add( String.join( ",", cols ) );
            }
            uniqueValues.add( data );
        }

        List<String> trueFalse = new ArrayList<>();
        trueFalse.add( "true" );
        trueFalse.add( "false" );
        uniqueValues.add( trueFalse );

        return uniqueValues;
    }


    /**
     * Gets all data with a query and adds "?" for classification
     *
     * @return List of all Data
     */
    private List<String[]> getAllData( String query ) {
        ExploreQueryResult exploreQueryResult = this.exploreQueryProcessor.executeSQL( query );
        List<String[]> allDataTable = new ArrayList<>( Arrays.asList( exploreQueryResult.data ) );
        allDataTable = rotate2dArray( allDataTable );

        String[] questionMark = new String[exploreQueryResult.count];
        for ( int j = 0; j < exploreQueryResult.count; j++ ) {
            questionMark[j] = "?";
        }
        allDataTable.add( questionMark );

        return allDataTable;
    }


    /**
     * Given a query it returns a list of all qualified column names
     *
     * @return list of all columns
     */
    private List<String> prepareColInfo( String query ) {
        return Arrays.asList( query.replace( "SELECT", "" ).split( "\nFROM" )[0].split( "," ) );
    }


    /**
     * @return the amount of entries of a given sql query
     */
    private int getSQLCount( String sql ) {
        ExploreQueryResult exploreQueryResult = this.exploreQueryProcessor.executeSQL( sql );
        return (exploreQueryResult.count);
    }


    private Map<String, Integer> getUniqueValuesCount( List<String> cols ) {
        Map<String, Integer> counts = new HashMap<>();
        cols.forEach( s -> counts.put( s, getUniqueValueCount( "SELECT DISTINCT" + s + "\nFROM " + s.replaceAll( "\\.[^.]*$", "" ) + "\nLIMIT 200" ) ) );
        return counts;
    }


    private Map<String, Integer> getMins( List<String> cols ) {
        Map<String, Integer> mins = new HashMap<>();
        cols.forEach( s -> mins.put( s, Integer.parseInt( getMin( "SELECT MIN(" + s + ") \nFROM " + s.replaceAll( "\\.[^.]*$", "" ) + "\nLIMIT 200 " ) ) ) );
        return mins;
    }


    private Map<String, Integer> getMaxs( List<String> cols ) {
        Map<String, Integer> maxs = new HashMap<>();
        cols.forEach( s -> maxs.put( s, Integer.parseInt( getMin( "SELECT MAX(" + s + ") \nFROM " + s.replaceAll( "\\.[^.]*$", "" ) + "\nLIMIT 200 " ) ) ) );
        return maxs;
    }


    private String getMin( String sql ) {
        ExploreQueryResult exploreQueryResult = this.exploreQueryProcessor.executeSQL( sql );
        return (exploreQueryResult.col);
    }


    private int getUniqueValueCount( String sql ) {
        ExploreQueryResult exploreQueryResult = this.exploreQueryProcessor.executeSQL( sql );
        return (exploreQueryResult.count);
    }


    /**
     * Rotates a {@code List<String[]>} to switch between rows and columns
     *
     * @param table of rows or columns
     * @return rotated table
     */
    private List<String[]> rotate2dArray( List<String[]> table ) {
        int width = table.get( 0 ).length;
        int height = table.size();

        String[][] rotatedTable = new String[width][height];

        for ( int x = 0; x < width; x++ ) {
            for ( int y = 0; y < height; y++ ) {
                rotatedTable[x][y] = table.get( y )[x];
            }
        }
        List<String[]> tab = new ArrayList<>();
        Collections.addAll( tab, rotatedTable );
        return tab;
    }


    /**
     * Creates a Weka instance for a given data table
     */
    public Instances createInstance( List<String[]> rotatedTable, List<String[]> table, String[] dataType, List<List<String>> uniqueValues ) {

        int numInstances = rotatedTable.get( 0 ).length;
        int dimLength = table.get( 0 ).length;
        FastVector atts = new FastVector();
        FastVector attVals;
        FastVector[] attValsEl = new FastVector[dimLength];
        Instances classifiedData;
        double[] vals;
        //fullNames.add("classification");

        // attributes
        for ( int dim = 0; dim < dimLength; dim++ ) {
            attVals = new FastVector();

            if ( Arrays.asList( "VARCHAR", "JSON" ).contains( dataType[dim] ) ) {
                for ( int i = 0; i < uniqueValues.get( dim ).size(); i++ ) {
                    attVals.addElement( uniqueValues.get( dim ).get( i ) );
                }
                atts.addElement( new Attribute( qualifiedNames.get( dim ), attVals ) );
                attValsEl[dim] = attVals;
            } else if ( Arrays.asList( "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "DECIMAL" ).contains( dataType[dim] ) ) {
                atts.addElement( new Attribute( qualifiedNames.get( dim ) ) );
            }
        }
        // instances object
        classifiedData = new Instances( "ClassifiedData", atts, 0 );

        // fill data classified
        for ( int obj = 0; obj < numInstances; obj++ ) {
            vals = new double[classifiedData.numAttributes()];

            for ( int dim = 0; dim < dimLength; dim++ ) {
                if ( dataType[dim].equals( "VARCHAR" ) ) {
                    if ( attValsEl[dim].contains( table.get( obj )[dim] ) ) {
                        vals[dim] = attValsEl[dim].indexOf( table.get( obj )[dim] );
                    } else {
                        vals[dim] = Utils.missingValue();
                    }
                } else if ( dataType[dim].equals( "INTEGER" ) || dataType[dim].equals( "BIGINT" ) || dataType[dim].equals( "SMALLINT" ) || dataType[dim].equals( "TINYINT" ) || dataType[dim].equals( "DECIMAL" ) ) {
                    vals[dim] = Double.parseDouble( table.get( obj )[dim] );
                }
            }
            classifiedData.add( new DenseInstance( 1.0, vals ) );
        }
        return classifiedData;
    }


    /**
     * Trains a J48 Tree with the entry selection from the user
     *
     * @param classifiedData weka Instances
     * @return J48 tree
     */
    public J48 trainData( Instances classifiedData ) {
        classifiedData.setClassIndex( classifiedData.numAttributes() - 1 );
        J48 tree = new J48();

        String[] options = { "-U" };
        try {
            tree.setOptions( options );
        } catch ( Exception e ) {
            log.error( "Caught exception while setting options for Classification", e );
        }

        try {
            tree.buildClassifier( classifiedData );
            this.buildGraph = tree.graph();
        } catch ( Exception e ) {
            log.error( "Caught exception while building Classifier and tree graph", e );
        }

        return tree;
    }


    /**
     * Translates a Weka J48 Tree to a SQL query
     *
     * @param tree J48 tree
     * @param nameAndType of all the columns
     * @return sql query
     */
    public String sqlClassifiedData( J48 tree, Map<String, String> nameAndType ) {
        String classifiedSqlStatement = query.split( "\nLIMIT" )[0] + WekaToSql.translate( tree.toString(), nameAndType, includesJoin );
        sqlStatement = classifiedSqlStatement;
        return classifiedSqlStatement;
    }


    /**
     * Classifies unlabeled data with the before created tree
     *
     * @param tree J48 tree
     * @param unlabeled Weka Instances
     * @return labeled data
     */
    public List<String[]> classifyUnlabeledData( J48 tree, Instances unlabeled ) {
        unlabeled.setClassIndex( unlabeled.numAttributes() - 1 );
        Instances labeled = new Instances( unlabeled );
        String[] label = new String[unlabeled.numInstances()];
        List<String[]> labeledData = new ArrayList<>();

        for ( int i = 0; i < unlabeled.numInstances(); i++ ) {
            double clsLabel = 0;

            try {
                clsLabel = tree.classifyInstance( unlabeled.instance( i ) );
            } catch ( Exception e ) {
                log.error( "Caught exception while classifying unlabeled data", e );
            }

            labeled.instance( i ).setClassValue( clsLabel );
            label[i] = unlabeled.classAttribute().value( (int) clsLabel );
            labeledData.add( labeled.instance( i ).toString().split( "," ) );
        }

        return labeledData;
    }


    /**
     * Classify all Data with tree built before
     *
     * @param tree J48 Weka Tree
     * @param unlabeled all selected unlabeled Data
     * @return only the data labeled as true
     */
    public String[][] classifyData( J48 tree, Instances unlabeled ) {
        List<String[]> labeledData = new ArrayList<>();
        unlabeled.setClassIndex( unlabeled.numAttributes() - 1 );
        Instances labeled = new Instances( unlabeled );

        for ( int i = 0; i < unlabeled.numInstances(); i++ ) {
            double clsLabel = 0;

            try {
                clsLabel = tree.classifyInstance( unlabeled.instance( i ) );
            } catch ( Exception e ) {
                log.error( "Caught exception while classifying all unlabeled data", e );
            }

            labeled.instance( i ).setClassValue( clsLabel );

            if ( "true".equals( unlabeled.classAttribute().value( (int) clsLabel ) ) ) {
                labeledData.add( Arrays.copyOf( labeled.instance( i ).toString().split( "," ), labeled.instance( i ).toString().split( "," ).length - 1 ) );
            }
        }
        return labeledData.toArray( new String[0][] );
    }

}
