/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.catalog.Catalog;


@Tag("adapter")
class MongoSourceRefreshTest {

    private static final String SUFFIX = UUID.randomUUID().toString().replace( "-", "" ).substring( 0, 8 );
    private static final String DATABASE = "mongo_refresh_" + SUFFIX;
    private static final String ADAPTER_NAME = "mongo_refresh_src_" + SUFFIX;
    private static final String INITIAL_COLLECTION = "students_" + SUFFIX;


    @Test
    void mongoSourceRefreshImportsNewCollection() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for MongoDB integration tests" );
        TestHelper.getInstance();

        String addedCollection = "courses_" + SUFFIX;

        try ( TestHelper.DockerMongo mongo = TestHelper.startMongoDocker( DATABASE ) ) {
            mongo.execute( "db." + INITIAL_COLLECTION + ".insertOne({ name: 'Alice' })" );

            TestHelper.addMongoSource( ADAPTER_NAME, mongo.getHost(), mongo.getPort(), DATABASE );

            try {
                long namespaceId = TestHelper.awaitDocumentNamespaceId( ADAPTER_NAME, 30 );
                TestHelper.awaitLogicalCollection( namespaceId, INITIAL_COLLECTION, 30 );

                mongo.execute( "db." + addedCollection + ".insertOne({ title: 'Databases' })" );

                long sourceId = TestHelper.awaitSourceAdapterId( ADAPTER_NAME, 30 );
                TestHelper.refreshSelectedSources( List.of( sourceId ) );

                TestHelper.awaitLogicalCollection( namespaceId, addedCollection, 30 );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + ADAPTER_NAME + "\"" );
            }
        }
    }


    @Test
    void mongoSourceRefreshDropsRemovedCollection() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for MongoDB integration tests" );
        TestHelper.getInstance();

        String removedCollection = "departments_" + SUFFIX;

        try ( TestHelper.DockerMongo mongo = TestHelper.startMongoDocker( DATABASE + "_drop" ) ) {
            mongo.execute( "db." + INITIAL_COLLECTION + ".insertOne({ name: 'Alice' })" );
            mongo.execute( "db." + removedCollection + ".insertOne({ name: 'CS' })" );

            String adapterName = ADAPTER_NAME + "_drop";
            TestHelper.addMongoSource( adapterName, mongo.getHost(), mongo.getPort(), DATABASE + "_drop" );

            try {
                long namespaceId = TestHelper.awaitDocumentNamespaceId( adapterName, 30 );
                TestHelper.awaitLogicalCollection( namespaceId, INITIAL_COLLECTION, 30 );
                TestHelper.awaitLogicalCollection( namespaceId, removedCollection, 30 );

                mongo.execute( "db." + removedCollection + ".drop()" );

                long sourceId = TestHelper.awaitSourceAdapterId( adapterName, 30 );
                TestHelper.refreshSelectedSources( List.of( sourceId ) );

                TestHelper.awaitLogicalCollectionAbsent( namespaceId, removedCollection, 30 );
                assertTrue( Catalog.snapshot().doc().getCollection( namespaceId, INITIAL_COLLECTION ).isPresent() );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapterName + "\"" );
            }
        }
    }


    @Test
    void mongoSourceRefreshOnlyAffectsSelectedSource() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for MongoDB integration tests" );
        TestHelper.getInstance();

        String adapterOne = ADAPTER_NAME + "_one";
        String adapterTwo = ADAPTER_NAME + "_two";
        String databaseOne = DATABASE + "_one";
        String databaseTwo = DATABASE + "_two";
        String sourceOneCollection = INITIAL_COLLECTION + "_one";
        String sourceTwoCollection = INITIAL_COLLECTION + "_two";
        String sourceOneAddedCollection = "courses_" + SUFFIX + "_one";
        String sourceTwoAddedCollection = "courses_" + SUFFIX + "_two";

        try ( TestHelper.DockerMongo mongoOne = TestHelper.startMongoDocker( databaseOne );
              TestHelper.DockerMongo mongoTwo = TestHelper.startMongoDocker( databaseTwo ) ) {
            mongoOne.execute( "db." + sourceOneCollection + ".insertOne({ name: 'Alice' })" );
            mongoTwo.execute( "db." + sourceTwoCollection + ".insertOne({ name: 'Bob' })" );

            TestHelper.addMongoSource( adapterOne, mongoOne.getHost(), mongoOne.getPort(), databaseOne );
            TestHelper.addMongoSource( adapterTwo, mongoTwo.getHost(), mongoTwo.getPort(), databaseTwo );

            try {
                long sourceOneNamespaceId = TestHelper.awaitDocumentNamespaceId( adapterOne, 30 );
                long sourceTwoNamespaceId = TestHelper.awaitDocumentNamespaceId( adapterTwo, 30 );
                TestHelper.awaitLogicalCollection( sourceOneNamespaceId, sourceOneCollection, 30 );
                TestHelper.awaitLogicalCollection( sourceTwoNamespaceId, sourceTwoCollection, 30 );

                mongoOne.execute( "db." + sourceOneAddedCollection + ".insertOne({ title: 'Databases' })" );
                mongoTwo.execute( "db." + sourceTwoAddedCollection + ".insertOne({ title: 'Algorithms' })" );

                long sourceOneId = TestHelper.awaitSourceAdapterId( adapterOne, 30 );
                TestHelper.refreshSelectedSources( List.of( sourceOneId ) );

                TestHelper.awaitLogicalCollection( sourceOneNamespaceId, sourceOneAddedCollection, 30 );
                assertFalse( Catalog.snapshot().doc().getCollection( sourceTwoNamespaceId, sourceTwoAddedCollection ).isPresent() );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapterOne + "\"" );
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapterTwo + "\"" );
            }
        }
    }


    @Test
    void mongoSourceRefreshOnlyAppliesMixedCollectionChangesToSelectedSource() throws Exception {
        Assumptions.assumeTrue( TestHelper.isLinuxDockerDaemonAvailable(), "A Linux Docker daemon is required for MongoDB integration tests" );
        TestHelper.getInstance();

        String adapterOne = ADAPTER_NAME + "_mixed_one";
        String adapterTwo = ADAPTER_NAME + "_mixed_two";
        String adapterThree = ADAPTER_NAME + "_mixed_three";
        String databaseOne = DATABASE + "_mixed_one";
        String databaseTwo = DATABASE + "_mixed_two";
        String databaseThree = DATABASE + "_mixed_three";

        String sourceOneInitialCollection = INITIAL_COLLECTION + "_mixed_one";
        String sourceTwoKeptCollection = INITIAL_COLLECTION + "_mixed_two";
        String sourceTwoRemovedCollection = "departments_" + SUFFIX + "_mixed_two";
        String sourceTwoAddedCollection = "courses_" + SUFFIX + "_mixed_two";
        String sourceTwoRenamedOldCollection = "projects_" + SUFFIX + "_mixed_old";
        String sourceTwoRenamedNewCollection = "projects_" + SUFFIX + "_mixed_new";
        String sourceThreeInitialCollection = INITIAL_COLLECTION + "_mixed_three";
        String sourceOneUnselectedAddedCollection = "courses_" + SUFFIX + "_mixed_one";
        String sourceThreeUnselectedAddedCollection = "courses_" + SUFFIX + "_mixed_three";

        try ( TestHelper.DockerMongo mongoOne = TestHelper.startMongoDocker( databaseOne );
              TestHelper.DockerMongo mongoTwo = TestHelper.startMongoDocker( databaseTwo );
              TestHelper.DockerMongo mongoThree = TestHelper.startMongoDocker( databaseThree ) ) {
            mongoOne.execute( "db." + sourceOneInitialCollection + ".insertOne({ name: 'Alice' })" );
            mongoTwo.execute( "db." + sourceTwoKeptCollection + ".insertOne({ name: 'Bob' })" );
            mongoTwo.execute( "db." + sourceTwoRemovedCollection + ".insertOne({ name: 'CS' })" );
            mongoTwo.execute( "db." + sourceTwoRenamedOldCollection + ".insertOne({ title: 'Legacy' })" );
            mongoThree.execute( "db." + sourceThreeInitialCollection + ".insertOne({ name: 'Carol' })" );

            TestHelper.addMongoSource( adapterOne, mongoOne.getHost(), mongoOne.getPort(), databaseOne );
            TestHelper.addMongoSource( adapterTwo, mongoTwo.getHost(), mongoTwo.getPort(), databaseTwo );
            TestHelper.addMongoSource( adapterThree, mongoThree.getHost(), mongoThree.getPort(), databaseThree );

            try {
                long sourceOneNamespaceId = TestHelper.awaitDocumentNamespaceId( adapterOne, 30 );
                long sourceTwoNamespaceId = TestHelper.awaitDocumentNamespaceId( adapterTwo, 30 );
                long sourceThreeNamespaceId = TestHelper.awaitDocumentNamespaceId( adapterThree, 30 );

                TestHelper.awaitLogicalCollection( sourceOneNamespaceId, sourceOneInitialCollection, 30 );
                TestHelper.awaitLogicalCollection( sourceTwoNamespaceId, sourceTwoKeptCollection, 30 );
                TestHelper.awaitLogicalCollection( sourceTwoNamespaceId, sourceTwoRemovedCollection, 30 );
                TestHelper.awaitLogicalCollection( sourceTwoNamespaceId, sourceTwoRenamedOldCollection, 30 );
                TestHelper.awaitLogicalCollection( sourceThreeNamespaceId, sourceThreeInitialCollection, 30 );

                mongoOne.execute( "db." + sourceOneUnselectedAddedCollection + ".insertOne({ title: 'Ignored' })" );
                mongoTwo.execute( "db." + sourceTwoAddedCollection + ".insertOne({ title: 'Databases' })" );
                mongoTwo.execute( "db." + sourceTwoRemovedCollection + ".drop()" );
                mongoTwo.execute( "db." + sourceTwoRenamedOldCollection + ".renameCollection('" + sourceTwoRenamedNewCollection + "')" );
                mongoThree.execute( "db." + sourceThreeUnselectedAddedCollection + ".insertOne({ title: 'Ignored Too' })" );

                long sourceTwoId = TestHelper.awaitSourceAdapterId( adapterTwo, 30 );
                TestHelper.refreshSelectedSources( List.of( sourceTwoId ) );

                TestHelper.awaitLogicalCollection( sourceTwoNamespaceId, sourceTwoAddedCollection, 30 );
                TestHelper.awaitLogicalCollection( sourceTwoNamespaceId, sourceTwoRenamedNewCollection, 30 );
                TestHelper.awaitLogicalCollectionAbsent( sourceTwoNamespaceId, sourceTwoRemovedCollection, 30 );
                TestHelper.awaitLogicalCollectionAbsent( sourceTwoNamespaceId, sourceTwoRenamedOldCollection, 30 );
                assertTrue( Catalog.snapshot().doc().getCollection( sourceTwoNamespaceId, sourceTwoKeptCollection ).isPresent() );

                assertFalse( Catalog.snapshot().doc().getCollection( sourceOneNamespaceId, sourceOneUnselectedAddedCollection ).isPresent() );
                assertFalse( Catalog.snapshot().doc().getCollection( sourceThreeNamespaceId, sourceThreeUnselectedAddedCollection ).isPresent() );
            } finally {
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapterOne + "\"" );
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapterTwo + "\"" );
                TestHelper.executeSQL( "ALTER ADAPTERS DROP \"" + adapterThree + "\"" );
            }
        }
    }

}
