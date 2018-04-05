/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.QueryEventListener;

import java.util.Map;
import java.util.logging.Logger;

import com.facebook.presto.spi.eventlistener.*;
import org.json.JSONObject;

public class QueryEventListener implements EventListener {
    Logger logger;

    public QueryEventListener() {
        createLogger();
    }

    public QueryEventListener(Map<String, String> config) {
        createLogger();
    }

    public void queryCreated(QueryCreatedEvent queryCreatedEvent) {
        JSONObject jsonString = new JSONObject();
        jsonString.put("Logger", "QueryEventListener");
        try {
            jsonString.put("EventType", "QueryCreate");
            jsonString.put("QueryId", queryCreatedEvent.getMetadata().getQueryId().toString());
            jsonString.put("QueryState", queryCreatedEvent.getMetadata().getQueryState().toString());
            jsonString.put("CreateTime", queryCreatedEvent.getCreateTime());
            jsonString.put("User", queryCreatedEvent.getContext().getUser().toString());
            jsonString.put("RemoteClientAddress", queryCreatedEvent.getContext().getRemoteClientAddress());
            jsonString.put("Principal", queryCreatedEvent.getContext().getPrincipal());
            jsonString.put("Source", queryCreatedEvent.getContext().getSource());
            jsonString.put("UserAgent", queryCreatedEvent.getContext().getUserAgent());
            jsonString.put("source", queryCreatedEvent.getContext().getSource());
            jsonString.put("Catelog", queryCreatedEvent.getContext().getCatalog());
            jsonString.put("Schema", queryCreatedEvent.getContext().getSchema());
            jsonString.put("ServerAddress", queryCreatedEvent.getContext().getServerAddress());
            logger.info(jsonString.toString());
        } catch (Exception ex) {
            jsonString.put("exception", ex.getMessage());
            logger.info(jsonString.toString());
        }
    }

    public void queryCompleted(QueryCompletedEvent queryCompletedEvent) {
        JSONObject jsonString = new JSONObject();
        jsonString.put("Logger", "QueryEventListener");
        try {
            jsonString.put("EventType", "QueryComplete");
            jsonString.put("QueryId", queryCompletedEvent.getMetadata().getQueryId().toString());
            jsonString.put("CreateTime", queryCompletedEvent.getCreateTime());
            jsonString.put("User", queryCompletedEvent.getContext().getUser().toString());
            jsonString.put("Complete", queryCompletedEvent.getStatistics().isComplete());
            jsonString.put("RemoteClientAddress", queryCompletedEvent.getContext().getRemoteClientAddress());
            jsonString.put("Query", queryCompletedEvent.getMetadata().getQuery());
            jsonString.put("Uri", queryCompletedEvent.getMetadata().getUri().toString());
            jsonString.put("state", queryCompletedEvent.getMetadata().getQueryState());
            jsonString.put("CpuTime", queryCompletedEvent.getStatistics().getCpuTime().toMillis());
            jsonString.put("WallTime", queryCompletedEvent.getStatistics().getWallTime().toMillis());
            jsonString.put("QueuedTime", queryCompletedEvent.getStatistics().getQueuedTime().toMillis());
            if(queryCompletedEvent.getStatistics().getAnalysisTime().isPresent()) {
                jsonString.put("AnalysisTime", queryCompletedEvent.getStatistics().getAnalysisTime().get().toMillis());
            }
            if(queryCompletedEvent.getStatistics().getDistributedPlanningTime().isPresent()) {
                jsonString.put("DistributedPlanningTime", queryCompletedEvent.getStatistics().getDistributedPlanningTime().get().toMillis());
            }
            jsonString.put("PeakMemoryBytes", queryCompletedEvent.getStatistics().getPeakMemoryBytes());
            jsonString.put("TotalBytes", queryCompletedEvent.getStatistics().getTotalBytes());
            jsonString.put("TotalRows", queryCompletedEvent.getStatistics().getTotalRows());
            jsonString.put("CompletedSplits", queryCompletedEvent.getStatistics().getCompletedSplits());
            if(queryCompletedEvent.getFailureInfo().isPresent()) {
                QueryFailureInfo queryFailureInfo = queryCompletedEvent.getFailureInfo().get();
                jsonString.put("errorCode", queryFailureInfo.getErrorCode());
                jsonString.put("failureHost", queryFailureInfo.getFailureHost().orElse(""));
                jsonString.put("failureMessage", queryFailureInfo.getFailureMessage().orElse(""));
                jsonString.put("failureTask", queryFailureInfo.getFailureTask().orElse(""));
                jsonString.put("failureType", queryFailureInfo.getFailureType().orElse(""));
                jsonString.put("failuresJson", queryFailureInfo.getFailuresJson());
                logger.info(jsonString.toString());
            }
            logger.info(jsonString.toString());
        } catch (Exception ex) {
            jsonString.put("exception", ex.getMessage());
            logger.info(jsonString.toString());
        }
    }

    public void splitCompleted(SplitCompletedEvent splitCompletedEvent) {
        JSONObject jsonString = new JSONObject();
        jsonString.put("Logger", "QueryEventListener");
        try {
            jsonString.put("EventType", "SplitComplete");
            jsonString.put("QueryId", splitCompletedEvent.getQueryId().toString());
            jsonString.put("StageId", splitCompletedEvent.getStageId().toString());
            jsonString.put("TaskId", splitCompletedEvent.getTaskId().toString());
            logger.info(jsonString.toString());
        } catch (Exception ex) {
            jsonString.put("exception", ex.getMessage());
            logger.info(jsonString.toString());
        }

    }

    public void createLogger() {
        logger = Logger.getLogger(QueryEventListener.class.getName());
    }
}
