/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.pipeline;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.job.JobRunner;
import org.apache.samza.pipeline.api.Pipeline;
import org.apache.samza.pipeline.api.StreamManager;
import org.apache.samza.pipeline.stream.DefaultStreamManager;
import org.apache.samza.pipeline.stream.PStream;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.Systems;


public class PipelineRunner {
  public static final String CONFIG_STREAM_PREFIX = "systems.%s.streams.%s."; // TODO do these belong in a PipelineConfig class?
  public static final String CONFIG_PROCESSOR_PREFIX = "processors.%s.";
  private final Pipeline pipeline;
  private final Config config;

  // TODO do we need config explicitly or does the pipeline?
  public PipelineRunner(Pipeline pipeline, Config config) {
    this.pipeline = pipeline;
    this.config = config;
  }

  public void run() {
    createStreams(pipeline, config);
    // todo implement
    for (Processor processor : pipeline.getProcessorsInStartOrder()) {
      startProcessor(processor);
    }
    // todo exceptions if any issue. Perhaps even roll back if we can't tolerate partial deployments.
  }

  private void createStreams(Pipeline pipeline, Config config) {
    // Build system -> streams map
    Multimap<String, PStream> streamsGroupedBySystem = HashMultimap.create();
    pipeline.getManagedStreams().forEach(pStream -> streamsGroupedBySystem.put(pStream.getSystem(), pStream));

    for (Map.Entry<String, Collection<PStream>> entry : streamsGroupedBySystem.asMap().entrySet()) {
      SystemAdmin systemAdmin = Systems.getSystemAdmins(config).get(entry.getKey());
      StreamManager streamManager = new DefaultStreamManager(systemAdmin);

      for (PStream stream : entry.getValue()) {
        createStream(config, streamManager, stream);
      }
    }
  }

  private void createStream(Config config, StreamManager streamManager, PStream stream) {
    Config streamConfig = extractStreamConfig(config, stream);
    streamManager.createOrUpdateStream(stream, streamConfig);
  }

  private void startProcessor(Processor processor) {
    Config procConfig = extractProcessorConfig(config, processor);
    JobRunner runner = new JobRunner(JobRunner.rewriteConfig(procConfig));
    runner.run(true);
  }

  private Config extractProcessorConfig(Config config, Processor processor) {
    return config.subset(String.format(CONFIG_PROCESSOR_PREFIX, processor.getName()));
  }

  private Config extractStreamConfig(Config config, PStream stream) {
    return config.subset(String.format(CONFIG_STREAM_PREFIX, stream.getSystem(), stream.getStream()));
  }
}