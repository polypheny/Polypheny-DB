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
import java.util.List;
import lombok.Getter;
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
    List<List<String>> uniqueValues;
    private String[][] labeled;
    private String[][] unlabeled;
    private String[] dataType;
    @Getter
    private String[] labels;
    private Instances unlabledData;
    @Getter
    private String buildGraph;
    @Getter
    private String[][] data;


    public Explore( int identifier, List<List<String>> uniqueValues, String[][] labeled, String[][] unlabeled, String[] dataType ) {
        this.id = identifier;
        this.uniqueValues = uniqueValues;
        this.labeled = labeled;
        this.unlabeled = unlabeled;
        this.dataType = dataType;
    }


    public void updateExploration( String[][] labeled ) {
        labels = classifyUnlabledData( trainData( createInstance( rotate2dArray( labeled ), labeled, dataType, uniqueValues ) ), unlabledData );
    }


    public void exploreUserInput() {
        unlabledData = createInstance( rotate2dArray( unlabeled ), unlabeled, dataType, uniqueValues );
        labels = classifyUnlabledData( trainData( createInstance( rotate2dArray( labeled ), labeled, dataType, uniqueValues ) ), unlabledData );
    }


    public void classifyAllData( String[][] labeled, String[][] allData ) {
        unlabledData = createInstance( allData, rotate2dArray( allData ), dataType, uniqueValues );
        data = classifyData( trainData( createInstance( rotate2dArray( labeled ), labeled, dataType, uniqueValues ) ), unlabledData );
    }


    private String[][] rotate2dArray( String[][] table ) {
        int width = table[0].length;
        int height = table.length;

        String[][] rotatedTable = new String[width][height];

        for ( int x = 0; x < width; x++ ) {
            for ( int y = 0; y < height; y++ ) {
                rotatedTable[x][y] = table[y][x];
            }
        }
        return rotatedTable;
    }


    public Instances createInstance( String[][] rotatedTable, String[][] table, String[] dataType, List<List<String>> uniqueValues ) {

        int numInstances = rotatedTable[0].length;
        int dimLength = table[0].length;
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
                    if ( attValsEl[dim].contains( table[obj][dim] ) ) {
                        vals[dim] = attValsEl[dim].indexOf( table[obj][dim] );
                    } else {
                        System.out.println( "i'm not inside of this else right?" );
                        vals[dim] = Utils.missingValue();
                    }
                } else if ( dataType[dim].equals( "INTEGER" ) || dataType[dim].equals( "BIGINT" ) ) {
                    vals[dim] = Double.parseDouble( table[obj][dim] );
                }
            }
            classifiedData.add( new DenseInstance( 1.0, vals ) );
        }
        return classifiedData;
    }


    public J48 trainData( Instances classifiedData ) {
        classifiedData.setClassIndex( classifiedData.numAttributes() - 1 );
        J48 tree = new J48();

        String[] options = { "-U" };
        try {
            tree.setOptions( options );
        } catch ( Exception e ) {

            e.printStackTrace();
        }

        try {
            tree.buildClassifier( classifiedData );
        } catch ( Exception e ) {
            e.printStackTrace();
        }

        try {
            this.buildGraph = tree.graph();
        } catch ( Exception e ) {
            e.printStackTrace();
        }

        return tree;
    }


    public String[] classifyUnlabledData( J48 tree, Instances unlabeled ) {

        unlabeled.setClassIndex( unlabeled.numAttributes() - 1 );
        Instances labeled = new Instances( unlabeled );
        String[] label = new String[unlabeled.numInstances()];

        for ( int i = 0; i < unlabeled.numInstances(); i++ ) {
            double clsLabel = 0;

            try {
                clsLabel = tree.classifyInstance( unlabeled.instance( i ) );
            } catch ( Exception e ) {
                e.printStackTrace();
            }

            labeled.instance( i ).setClassValue( clsLabel );
            label[i] = unlabeled.classAttribute().value( (int) clsLabel );
        }

        return label;
    }


    public String[][] classifyData( J48 tree, Instances unlabeled ) {
        List<String[]> labledData = new ArrayList<>();
        unlabeled.setClassIndex( unlabeled.numAttributes() - 1 );
        Instances labeled = new Instances( unlabeled );

        for ( int i = 0; i < unlabeled.numInstances(); i++ ) {
            double clsLabel = 0;

            try {
                clsLabel = tree.classifyInstance( unlabeled.instance( i ) );
            } catch ( Exception e ) {
                e.printStackTrace();
            }

            labeled.instance( i ).setClassValue( clsLabel );

            if ( "true".equals( unlabeled.classAttribute().value( (int) clsLabel ) ) ) {
                labledData.add( Arrays.copyOf( labeled.instance( i ).toString().split( "," ), labeled.instance( i ).toString().split( "," ).length - 1 ) );
            }
        }
        return labledData.toArray( new String[0][] );
    }

}
