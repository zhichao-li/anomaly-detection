/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.ml;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.serialize.RandomCutForestSerDe;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * DAO for model checkpoints.
 */
public class CheckpointDao {

    protected static final String DOC_TYPE = "_doc";
    protected static final String FIELD_MODEL = "model";
    public static final String TIMESTAMP = "timestamp";
    protected static final String ENTITY_SAMPLE = "sp";
    protected static final String ENTITY_RCF = "rcf";
    protected static final String ENTITY_THRESHOLD = "th";

    private static final Logger logger = LogManager.getLogger(CheckpointDao.class);

    // dependencies
    private final Client client;
    private final ClientUtil clientUtil;

    // configuration
    private final String indexName;

    private Gson gson;
    private RandomCutForestSerDe rcfSerde;
    private final Class<? extends ThresholdingModel> thresholdingModelClass;

    /**
     * Constructor with dependencies and configuration.
     *
     * @param client ES search client
     * @param clientUtil utility with ES client
     * @param indexName name of the index for model checkpoints
     * @param gson accessor to Gson functionality
     * @param rcfSerde accessor to rcf serialization/deserialization
     * @param thresholdingModelClass thresholding model's class
     */
    public CheckpointDao(
        Client client,
        ClientUtil clientUtil,
        String indexName,
        Gson gson,
        RandomCutForestSerDe rcfSerde,
        Class<? extends ThresholdingModel> thresholdingModelClass
    ) {
        this.client = client;
        this.clientUtil = clientUtil;
        this.indexName = indexName;
        this.gson = gson;
        this.rcfSerde = rcfSerde;
        this.thresholdingModelClass = thresholdingModelClass;
    }

    /**
     * Puts a model checkpoint in the storage.
     *
     * @deprecated use putModelCheckpoint with listener instead
     *
     * @param modelId Id of the model
     * @param modelCheckpoint Checkpoint data of the model
     */
    @Deprecated
    public void putModelCheckpoint(String modelId, String modelCheckpoint) {
        Map<String, Object> source = new HashMap<>();
        source.put(FIELD_MODEL, modelCheckpoint);
        source.put(TIMESTAMP, ZonedDateTime.now(ZoneOffset.UTC));

        clientUtil
            .<IndexRequest, IndexResponse>timedRequest(
                new IndexRequest(indexName, DOC_TYPE, modelId).source(source),
                logger,
                client::index
            );
    }

    /**
     * Puts a model checkpoint in the storage.
     *
     * @param modelId id of the model
     * @param modelCheckpoint checkpoint of the model
     * @param listener onResponse is called with null when the operation is completed
     */
    public void putModelCheckpoint(String modelId, String modelCheckpoint, ActionListener<Void> listener) {
        Map<String, Object> source = new HashMap<>();
        source.put(FIELD_MODEL, modelCheckpoint);
        source.put(TIMESTAMP, ZonedDateTime.now(ZoneOffset.UTC));
        clientUtil
            .<IndexRequest, IndexResponse>asyncRequest(
                new IndexRequest(indexName, DOC_TYPE, modelId).source(source),
                client::index,
                ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
            );
    }

    public void bulk(BulkRequest bulkRequest) {
        // TODO: add retry logic
        // It is possible other concurrent save has already depleted toSave
        if (bulkRequest.numberOfActions() > 0) {
            clientUtil
                .<BulkRequest, BulkResponse>execute(
                    BulkAction.INSTANCE,
                    bulkRequest,
                    ActionListener
                        .wrap(r -> logger.debug("Succeeded in bulking checkpoints"), e -> logger.error("Failed bulking checkpoints", e))
                );
        }
    }

    public void prepareBulk(BulkRequest bulkRequest, EntityModel modelState, String modelId) {
        Map<String, Object> source = new HashMap<>();
        source.put(FIELD_MODEL, toCheckpoint(modelState));
        bulkRequest.add(new IndexRequest(indexName, DOC_TYPE, modelId).source(source));
    }

    /**
     * Returns the checkpoint for the model.
     *
     * @deprecated use getModelCheckpoint with listener instead
     *
     * @param modelId ID of the model
     * @return model checkpoint, or empty if not found
     */
    @Deprecated
    public Optional<String> getModelCheckpoint(String modelId) {
        return clientUtil
            .<GetRequest, GetResponse>timedRequest(new GetRequest(indexName, DOC_TYPE, modelId), logger, client::get)
            .filter(GetResponse::isExists)
            .map(GetResponse::getSource)
            .map(source -> (String) source.get(FIELD_MODEL));
    }

    /**
     * Returns to listener the checkpoint for the model.
     *
     * @param modelId id of the model
     * @param listener onResponse is called with the model checkpoint, or empty for no such model
     */
    public void getModelCheckpoint(String modelId, ActionListener<Optional<String>> listener) {
        clientUtil
            .<GetRequest, GetResponse>asyncRequest(
                new GetRequest(indexName, DOC_TYPE, modelId),
                client::get,
                ActionListener.wrap(response -> listener.onResponse(processModelCheckpoint(response)), listener::onFailure)
            );
    }

    private Optional<String> processModelCheckpoint(GetResponse response) {
        return Optional
            .ofNullable(response)
            .filter(GetResponse::isExists)
            .map(GetResponse::getSource)
            .map(source -> (String) source.get(FIELD_MODEL));
    }

    public void restoreModelCheckpoint(String modelId, ActionListener<Optional<EntityModel>> listener) {
        clientUtil
            .<GetRequest, GetResponse>asyncRequest(
                new GetRequest(indexName, DOC_TYPE, modelId),
                client::get,
                ActionListener.wrap(response -> {
                    Optional<String> checkpointString = processModelCheckpoint(response);
                    if (checkpointString.isPresent()) {
                        listener.onResponse(Optional.of(fromEntityModelCheckpoint(checkpointString.get(), modelId)));
                    } else {
                        listener.onResponse(Optional.empty());
                    }
                }, listener::onFailure)
            );
    }

    private String toCheckpoint(EntityModel model) {
        return AccessController.doPrivileged((PrivilegedAction<String>) () -> {
            JsonObject json = new JsonObject();
            json.add(ENTITY_SAMPLE, gson.toJsonTree(model.getSamples()));
            if (model.getRcf() != null) {
                json.addProperty(ENTITY_RCF, rcfSerde.toJson(model.getRcf()));
            }
            if (model.getThreshold() != null) {
                json.addProperty(ENTITY_THRESHOLD, gson.toJson(model.getThreshold()));
            }
            return gson.toJson(json);
        });
    }

    private EntityModel fromEntityModelCheckpoint(String checkpoint, String modelId) {
        try {
            return AccessController.doPrivileged((PrivilegedAction<EntityModel>) () -> {
                JsonObject json = new JsonParser().parse(checkpoint).getAsJsonObject();
                ArrayDeque<double[]> samples = new ArrayDeque<>(
                    Arrays.asList(this.gson.fromJson(json.getAsJsonArray(ENTITY_SAMPLE), new double[0][0].getClass()))
                );
                RandomCutForest rcf = null;
                if (json.has(ENTITY_RCF)) {
                    rcf = rcfSerde.fromJson(json.getAsJsonPrimitive(ENTITY_RCF).getAsString());
                }
                ThresholdingModel threshold = null;
                if (json.has(ENTITY_THRESHOLD)) {
                    threshold = this.gson.fromJson(json.getAsJsonPrimitive(ENTITY_THRESHOLD).getAsString(), thresholdingModelClass);
                }
                return new EntityModel(modelId, samples, rcf, threshold);
            });
        } catch (RuntimeException e) {
            logger.warn("from", e);
            throw e;
        }
    }

    /**
     * Deletes the model checkpoint for the id.
     *
     * @deprecated use deleteModelCheckpoint with listener instead
     *
     * @param modelId ID of the model checkpoint
     */
    @Deprecated
    public void deleteModelCheckpoint(String modelId) {
        clientUtil.<DeleteRequest, DeleteResponse>timedRequest(new DeleteRequest(indexName, DOC_TYPE, modelId), logger, client::delete);
    }

    /**
     * Deletes the model checkpoint for the model.
     *
     * @param modelId id of the model
     * @param listener onReponse is called with null when the operation is completed
     */
    public void deleteModelCheckpoint(String modelId, ActionListener<Void> listener) {
        clientUtil
            .<DeleteRequest, DeleteResponse>asyncRequest(
                new DeleteRequest(indexName, DOC_TYPE, modelId),
                client::delete,
                ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
            );
    }
}
