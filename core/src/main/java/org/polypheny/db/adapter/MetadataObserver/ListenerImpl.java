/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.adapter.MetadataObserver;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.MetadataObserver.ChangeLogEntry.DiffMessageUtil;
import org.polypheny.db.adapter.MetadataObserver.PublisherManager.ChangeStatus;
import org.polypheny.db.adapter.MetadataObserver.Utils.MetaAnnotator;
import org.polypheny.db.adapter.MetadataObserver.Utils.MetaDiffUtil;
import org.polypheny.db.adapter.MetadataObserver.Utils.MetaDiffUtil.DiffResult;
import org.polypheny.db.adapter.java.AdapterTemplate.PreviewResultEntry;
import org.polypheny.db.schemaDiscovery.AbstractNode;
import org.polypheny.db.schemaDiscovery.MetadataProvider;
import org.polypheny.db.schemaDiscovery.NodeSerializer;
import org.polypheny.db.schemaDiscovery.NodeUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

@Slf4j
public class ListenerImpl<P extends Adapter & MetadataProvider> implements AbstractListener<P> {

    private boolean available;
    private AbstractNode currentNode;
    private P adapter;
    private String hash;

    private static AbstractNode formRootNode = null;

    private static final Gson GSON = new Gson();


    public ListenerImpl() {
        available = true;
        currentNode = null;
        this.adapter = null;
        this.hash = null;
    }


    @Override
    public void onMetadataChange(P adapter, AbstractNode node, String hash) {
        available ^= true;
        this.currentNode = node;
        this.adapter = adapter;
        this.hash = hash;

        Object preview = adapter.getPreview();


        DiffResult result = MetaDiffUtil.diff(adapter.getRoot(), node);

        ChangeStatus status = NodeUtil.evaluateStatus(result, adapter.getRoot());

        ChangeLogEntry entry = new ChangeLogEntry(adapter.getUniqueName(), Instant.now(), DiffMessageUtil.toMessages(result), status);
        PublisherManager.getInstance().addChange(entry);

        AbstractNode annotatedCopy = MetaAnnotator.annotateTree(adapter.getRoot(), node, result);
        String json = NodeSerializer.serializeNode(annotatedCopy).toString();

        PublisherManager.getInstance().onMetadataChange(adapter.getUniqueName(), new PreviewResultEntry(json, preview, List.of(entry)), status);
    }


    public static PreviewResultEntry buildFormChange(String uniqueName, AbstractNode oldRoot, AbstractNode newRoot, Object preview, String path) {
        DiffResult diff = MetaDiffUtil.diff(oldRoot, newRoot);
        ChangeStatus status = NodeUtil.evaluateStatus(diff, oldRoot);

        ChangeLogEntry entry = new ChangeLogEntry(uniqueName, Instant.now(), DiffMessageUtil.toMessages(diff), status);

        AbstractNode annotated = MetaAnnotator.annotateTree(oldRoot, newRoot, diff);
        String json = NodeSerializer.serializeNode(annotated).toString();

        PublisherManager pm = PublisherManager.getInstance();
        pm.addChange(entry);
        PreviewResultEntry result = new PreviewResultEntry(json, preview, List.of(entry));
        pm.onMetadataChange(uniqueName, result, status);
        pm.saveTempPath(uniqueName, path);

        formRootNode = newRoot;

        return result;
    }


    public static void applyAnnotatedTree(Adapter<?> adapter, AbstractNode newRoot, String newHash, String[] additionallySelectedMetadata) {

        if (!(adapter instanceof DataSource)) {
            throw new IllegalArgumentException("Adapter must be of type DataSource");
        }

        MetadataProvider metadataProvider = (MetadataProvider) adapter;

        Set<String> selected = NodeUtil.collectSelecedAttributePaths(metadataProvider.getRoot());
        if (additionallySelectedMetadata != null) {
            selected.addAll(Arrays.asList(additionallySelectedMetadata));
        }

        metadataProvider.setRoot(newRoot);
        metadataProvider.markSelectedAttributes(List.copyOf(selected));
        HashCache.getInstance().put(adapter.getUniqueName(), newHash);
    }


    @Override
    public void applyChange(String[] metadata) {
        Set<String> prevSelected = NodeUtil.collectSelecedAttributePaths(this.adapter.getRoot());

        this.adapter.setRoot(this.currentNode);
        if (metadata != null && metadata.length > 0) {
            prevSelected.addAll(Arrays.asList(metadata));
        }
        this.adapter.markSelectedAttributes(List.copyOf(prevSelected));
        HashCache.getInstance().put(this.adapter.getUniqueName(), this.hash);

        this.currentNode = null;
        this.adapter = null;
        this.hash = null;

        available ^= true;

    }


    // CSV and Excel does not support observer deployment. Therefore, a manual approach with a reupload is necessary to update data.
    public static void applyFormChange(String[] metadata, String uniqueName, String newPath) {
        AbstractNode newRoot = formRootNode;

        DataSource<?> adapter = AdapterManager.getInstance().getSource(uniqueName).orElseThrow();
        MetadataProvider metadataprovider = (MetadataProvider) adapter;

        deleteTempPath(newPath, adapter.getSettings().get("directory"));

        newRoot = metadataprovider.fetchMetadataTree();

        AbstractNode oldRoot = metadataprovider.getRoot();
        metadataprovider.setRoot(newRoot);

        Set<String> prevSelected = NodeUtil.collectSelecedAttributePaths(oldRoot);
        // metadataprovider.setRoot( newRoot );
        if (metadata != null && metadata.length > 0) {
            prevSelected.addAll(Arrays.asList(metadata));
        }

        metadataprovider.markSelectedAttributes(List.copyOf(prevSelected));

        formRootNode = null;
        PublisherManager.getInstance().deleteTempPath(uniqueName);

    }


    // Changing the old file on the directory with the new/temporary file.
    private static void deleteTempPath(String tmpPath, String directory) {
        File tmpDir = new File(tmpPath);
        File targetDir = new File(directory);

        if (!tmpDir.exists() || !tmpDir.isDirectory()) {
            throw new IllegalArgumentException("tmpPath is not a valid directory: " + tmpPath);
        }
        if (!targetDir.exists() || !targetDir.isDirectory()) {
            throw new IllegalArgumentException("directory is not a valid directory: " + directory);
        }

        for (File file : targetDir.listFiles()) {
            if (!file.delete()) {
                throw new RuntimeException("Failed to delete file: " + file.getAbsolutePath());
            }
        }

        for (File file : tmpDir.listFiles()) {
            try {
                Files.copy(file.toPath(), new File(targetDir, file.getName()).toPath(),
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new RuntimeException("Failed to copy file: " + file.getAbsolutePath(), e);
            }
        }

        for (File file : tmpDir.listFiles()) {
            file.delete();
        }
        if (!tmpDir.delete()) {
            throw new RuntimeException("Failed to delete tmpPath directory: " + tmpDir.getAbsolutePath());
        }
    }


    @Override
    public boolean isAvailable() {
        return this.available;
    }

}
