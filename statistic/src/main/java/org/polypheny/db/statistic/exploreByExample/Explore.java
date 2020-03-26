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
 */

package org.polypheny.db.statistic.exploreByExample;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
    @Setter
    private List<String[]> labeled;
    @Setter
    private List<String[]> unlabeled;
    @Getter
    @Setter
    private String[] dataType;
    @Setter
    @Getter
    private String[] labels;
    private Instances unlabledData;
    @Getter
    private String buildGraph;
    @Getter
    private String[][] data;
    @Getter
    private String query;
    Map<String, List<String>> sqlValues;
    private String [][] classifiedData;
    List<String> typeInfo;
    @Getter
    private String sqlStatment;

    @Getter
    private List<String[]> dataAfterClassification;


    public Explore( int identifier, List<List<String>> uniqueValues, List<String[]> labeled, List<String[]> unlabeled, String[] dataType ) {
        this.id = identifier;
        this.uniqueValues = uniqueValues;
        this.labeled = labeled;
        this.unlabeled = unlabeled;
        this.dataType = dataType;
    }

    public Explore(int identifier, String query, List<String> typeInfo){
        this.id = identifier;
        this.query = query;
        this.sqlStatment = query;
        this.typeInfo = typeInfo;
    }

    public void updateExploration( List<String[]> labeled) {
        dataAfterClassification = classifyUnlabledData( trainData(createInstance(rotate2dArray( labeled ), labeled, dataType, uniqueValues)), unlabledData );
    }

    public void exploreUserInput() {
        unlabledData = createInstance(rotate2dArray( unlabeled ), unlabeled, dataType, uniqueValues);
        dataAfterClassification = classifyUnlabledData( trainData(createInstance(rotate2dArray( labeled ), labeled, dataType, uniqueValues)), unlabledData );
    }

    public void classifyAllData( List<String[]> labeled, List<String[]> allData ) {
        unlabledData = createInstance( allData, rotate2dArray( allData ), dataType, uniqueValues);
        data = classifyData( trainData(createInstance(rotate2dArray( labeled ), labeled, dataType, uniqueValues)), unlabledData );
    }

    //TODO: Isabel change SQL Statment, now really ugly and min missing
    public void createSQLStatement(){

        List<String> q = new ArrayList<>(  );
        List<String> list = new ArrayList<>(  );
        List<String> list2= new ArrayList<>(  );


        q = Arrays.asList( sqlStatment.replace( "SELECT", "" ).split( "\nFROM" )[0].split( "," ) );


        for ( int i = 0; i < q.size(); i ++){
            if(typeInfo.get( i ).equals( "INTEGER" )){
                list2.add( "MAX(" + q.get( i ) + ") AS MAXi" + i + " " );
                //list2.add( "MIN(" + q.get( i ) + ") AS MINi" + i + " " );
            }
            if(typeInfo.get( i ).equals( "VARCHAR" )){
                list.add( q.get( i ) );
                list2.add( q.get( i ) );
            }
        }

        String listString = String.join(",", list);
        String listString2 = String.join(",", list2);

        sqlStatment = sqlStatment.split( "\nFROM" )[1];
        sqlStatment = "SELECT " + listString2 + "\nFROM" + sqlStatment + "\nGROUP BY " + listString + " LIMIT 200";
    }

    private List<String[]> rotate2dArray( List<String[]> table ) {
        int width = table.get( 0 ).length;
        int height = table.size();

        String[][] rotatedTable = new String[width][height];

        for ( int x = 0; x < width; x++ ) {
            for ( int y = 0; y < height; y++ ) {
                rotatedTable[x][y] = table.get( y )[x];
            }
        }
        List<String[]> tab = new ArrayList<>(  );
        Collections.addAll( tab, rotatedTable );
        return tab;
    }

    public Instances createInstance( List<String[]> rotatedTable, List<String[]> table, String[] dataType, List<List<String>> uniqueValues ){

        int numInstances = rotatedTable.get( 0 ).length;
        int dimLength = table.get( 0 ).length;
        FastVector atts = new FastVector();
        FastVector attVals;
        FastVector attValsEl[] = new FastVector[dimLength];
        Instances classifiedData;
        Instances allData;
        double[] vals;

        // attributes
        for ( int dim = 0; dim < dimLength; dim++ ) {
            attVals = new FastVector();

            if ( dataType[dim].equals( "VARCHAR" ) ) {
                for ( int i = 0; i < uniqueValues.get( dim ).size(); i++ ) {
                    attVals.addElement( uniqueValues.get( dim ).get( i ) );
                }
                atts.addElement( new Attribute( "attr" + dim, attVals ) );
                attValsEl[dim] = attVals;
            } else if ( dataType[dim].equals( "INTEGER" ) || dataType[dim].equals( "BIGINT" ) ) {
                atts.addElement( new Attribute( "attr" + dim ) );
            }
        }
        // instances object
        classifiedData = new Instances( "ClassifiedData", atts, 0 );

        // fill data classified
        for ( int obj = 0; obj < numInstances; obj++ ) {
            vals = new double[classifiedData.numAttributes()];

            for ( int dim = 0; dim < dimLength; dim++ ) {
                if ( dataType[dim].equals( "VARCHAR" ) ) {
                    if (attValsEl[dim].contains( table.get( obj )[dim] )){
                        vals[dim] = attValsEl[dim].indexOf( table.get( obj )[dim] );
                    }
                    else {
                        vals[dim] = Utils.missingValue();
                    }
                } else if ( dataType[dim].equals( "INTEGER" ) || dataType[dim].equals( "BIGINT" ) ) {
                    vals[dim] = Double.parseDouble( table.get( obj )[dim] );
                }
            }
            classifiedData.add( new DenseInstance( 1.0, vals ) );
        }
        return classifiedData;
    }

    public J48 trainData( Instances classifiedData){
        classifiedData.setClassIndex( classifiedData.numAttributes() - 1 );
        J48 tree = new J48();

        String[] options = { "-U" };
        try {
            tree.setOptions( options );
        } catch ( Exception e ) {
            log.error("setting options for Classification not possible");
            e.printStackTrace();
        }

        try {
            tree.buildClassifier( classifiedData );
        } catch ( Exception e ) {
            log.error("build Classifier for tree not possible");
            e.printStackTrace();
        }

        try {
            this.buildGraph = tree.graph();
        } catch ( Exception e ) {
            log.error("build graph tree not possible");
            e.printStackTrace();
        }

        return tree;
    }

    public List<String[]> classifyUnlabledData( J48 tree, Instances unlabeled ){

        unlabeled.setClassIndex( unlabeled.numAttributes() - 1 );
        Instances labeled = new Instances( unlabeled );
        String[] label = new String[unlabeled.numInstances()];
        List<String[]> labledData = new ArrayList<>();

        for ( int i = 0; i < unlabeled.numInstances(); i++ ) {
            double clsLabel = 0;

            try {
                clsLabel = tree.classifyInstance( unlabeled.instance( i ) );
            } catch ( Exception e ) {
                log.error("classification of unlabeled data not possible");
                e.printStackTrace();
            }

            labeled.instance( i ).setClassValue( clsLabel );
            label[i] = unlabeled.classAttribute().value( (int) clsLabel );
            labledData.add( labeled.instance( i ).toString().split( "," ) );
        }

        return labledData;
    }


    /**
     * Classify all Data with tree built before
     * @param tree J48 Weka Tree
     * @param unlabeled all selected unlabeled Data
     * @return only the data labeled as true
     */
    public String[][] classifyData( J48 tree, Instances unlabeled ){
        List<String[]> labledData = new ArrayList<>();
        unlabeled.setClassIndex( unlabeled.numAttributes() - 1 );
        Instances labeled = new Instances( unlabeled );

        for ( int i = 0; i < unlabeled.numInstances(); i++ ) {
            double clsLabel = 0;

            try {
                clsLabel = tree.classifyInstance( unlabeled.instance( i ) );
            } catch ( Exception e ) {
                log.error("classification of unlabeled data not possible");
                e.printStackTrace();
            }

            labeled.instance( i ).setClassValue( clsLabel );

            if ( "true".equals( unlabeled.classAttribute().value( (int) clsLabel ) ) ) {
                 labledData.add(Arrays.copyOf( labeled.instance( i ).toString().split( "," ), labeled.instance( i ).toString().split( "," ).length - 1 ));
            }
        }
        return labledData.toArray( new String[0][] );
    }

}
