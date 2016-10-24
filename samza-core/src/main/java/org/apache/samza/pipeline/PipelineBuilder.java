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
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import java.util.HashSet;
import java.util.Set;
import org.apache.samza.pipeline.api.KeyExtractor;
import org.apache.samza.pipeline.api.Pipeline;
import org.apache.samza.pipeline.stream.PStream;
import org.apache.samza.system.SystemStream;


public class PipelineBuilder {
  MutablePipeline pipeline = new MutablePipeline();

  private String templateSystem = "kafka";
  private int templatePartitionCount;
  private KeyExtractor templateKeyExtractor;

  /******************************************** Simple API ******************************************************/
  /**
   * Builds the topology.
   *
   * @return an instance of {@link Pipeline}
   */
  public Pipeline build() {
    // TODO it would be nice to set the type and visibility attributes automatically here.
    // Since we know most of them by position. The notable exception is that intermediate streams could be public
    return new BasePipeline(Lists.newArrayList(pipeline.processors),
                            Lists.newArrayList(pipeline.streams),
                            pipeline.streamProducers.asMap(),
                            pipeline.streamConsumers.asMap());
  }


  /**
   * Primary method to add a processor to the topology.
   *
   * TODO
   * @param processorName the processor to add to the topology.
   * @return          this builder, for chaining.
   */
  public PipelineBuilder addProcessor(String processorName) {
    addProcessor(createProcessor(processorName));
    return this;
  }

  /**
   * TODO if we had a stream template that knew the system, partition count and key
   * @param processorName
   * @param streamNames
   * @return
   */
  public PipelineBuilder addOutputStreams(String processorName, String... streamNames) {
    Processor processor = createProcessor(processorName);
    for (String streamName : streamNames) {
      PStream stream = createStream(streamName, PStream.Type.SINK, PStream.Visibility.PUBLIC);
      addOutputStreams(processor, stream);
    }
    return this;
  }

  public PipelineBuilder addIntermediateStreams(String producerName, String consumerName, String... streamNames) {
    Processor producer = createProcessor(producerName);
    Processor consumer = createProcessor(consumerName);
    PStream[] streams = new PStream[streamNames.length];
    for (int i = 0; i < streamNames.length; i++) {
      streams[i] = createStream(streamNames[i], PStream.Type.INTERMEDIATE, PStream.Visibility.PRIVATE);
    }
    addIntermediateStreams(producer, consumer, streams);
    return this;
  }

  public PipelineBuilder setPublic(String streamName) {
//    // TODO need to fetch the orignal and remove it from all datastructures.
//    PStream stream = PStream.fromTemplate(original, PStream.Visibility.PUBLIC);
//    // TODO make a copy and replace the origninal in all data structures
    return this;
  }

  /**
   *
   * @param processorName
   * @param streamNames
   * @return
   */
  public PipelineBuilder addInputStreams(String processorName, String... streamNames) {
    Processor processor = createProcessor(processorName);
    for (String streamName : streamNames) {
      PStream stream = createStream(streamName, PStream.Type.SOURCE, PStream.Visibility.PUBLIC);
      addInputStreams(processor, stream);
    }
    return this;
  }

  public PStream addStream(String streamName) {
    PStream stream = new PStream(new SystemStream(templateSystem, streamName), templatePartitionCount,
        templateKeyExtractor);
    addStream(stream);
    return stream;
  }

  private PStream createStream(String streamName, PStream.Type type, PStream.Visibility visibility) {
    PStream stream = new PStream(new SystemStream(templateSystem, streamName), templatePartitionCount,
        templateKeyExtractor, type, visibility);
    return stream;
  }

  private Processor createProcessor(String processorName) {
    return new Processor(processorName);
  }


















  /******************************************* Advanced API ****************************************************/
  // TODO add stream spec or default config which can be used to simplify the api for adding streams
  // TODO add processor spec or default config which can be used to simplify the api for adding processors.


  /**
   * Adds a processor, initially with no streams.
   *
   * TODO
   * @param processor the processor to add to the topology.
   * @return          this builder, for chaining.
   */
  public PipelineBuilder addProcessor(Processor processor) {
    pipeline.processors.add(processor);
    return this;
  }

  /**
   * Adds a processor (if necessary) and associates a list of streams as inputs.
   *
   * @param processor
   * @param streams
   * @return
   */
  // TODO: Alternate name could be addHeadProcessor if we want users to focus on processors rather than streams.
  public PipelineBuilder addInputStreams(Processor processor, PStream... streams) {
    addProcessor(processor);
    for (PStream stream: streams) {
      addStream(stream);
      pipeline.streamConsumers.put(stream, processor);
    }
    return this;
  }

  /**
   * Adds two processors (if necessary) and connects them with the list of streams.
   *
   * @param producer
   * @param consumer
   * @param streams
   * @return
   */
  // TODO Alternate name could be addDownstreamProcessor(producer, consumer, streams...) if we want users to focus on processors rather than streams.
  // TODO it's confusing to refer to input, intermediate and output streams when the pipeline terminology is source, int, sink
  // TODO maybe it should be:
  // addConsumer() or addProcessorWithInputs()
  // addProducerConsumer()
  // addProducer() or addProcessorWithOutputs()
  public PipelineBuilder addIntermediateStreams(Processor producer, Processor consumer, PStream... streams) {
    addProcessor(producer);
    addProcessor(consumer);
    for (PStream stream: streams) {
      addStream(stream);
      pipeline.streamProducers.put(stream, producer);
      pipeline.streamConsumers.put(stream, consumer);
    }
    return this;
  }

  /**
   * Adds a processor (if necessary) and associates a list of streams as outputs.
   *
   * TODO if we had a stream template that knew the system, partition count and key
   * @param processor
   * @param streams
   * @return
   */
  // TODO: Alternate name could be addTailProcessor if we want users to focus on processors rather than streams.
  public PipelineBuilder addOutputStreams(Processor processor, PStream... streams) {
    addProcessor(processor);
    for (PStream stream : streams) {
      addStream(stream);
      pipeline.streamProducers.put(stream, processor);
    }
    return this;
  }

  private PipelineBuilder addStream(PStream stream) {
    pipeline.streams.add(stream);
    return this;
  }

  private static class MutablePipeline {
    private final Set<Processor> processors = new HashSet<>(); // should be initialized with an unmodifiable list
    private final Set<PStream> streams = new HashSet<>(); // should be initialized with an unmodifiable list

    private final Multimap<PStream, Processor> streamProducers = HashMultimap.create(); // Map from stream name to its producers
    private final Multimap<PStream, Processor> streamConsumers = HashMultimap.create(); // Map from stream name to its consumers
  }
}
