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

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.pipeline.api.Pipeline;
import org.apache.samza.pipeline.stream.PStream;


/**
 * TODO
 */
public final class BasePipeline implements Pipeline {
  // TODO should have name-id just like jobs. Should this come from config or code? (leaning config)
  private final List<Processor> processors; // should be initialized with an unmodifiable list
  private final List<PStream> streams; // should be initialized with an unmodifiable list
  private final Map<PStream, Collection<Processor>> streamProducers; // Map from stream name to its producers
  private final Map<PStream, Collection<Processor>> streamConsumers; // Map from stream name to its consumers

  protected BasePipeline(List<Processor> processors,
                      List<PStream> streams,
                      Map<PStream, Collection<Processor>> streamProducers,
                      Map<PStream, Collection<Processor>> streamConsumers) {

    this.processors = Collections.unmodifiableList(processors);
    this.streams = Collections.unmodifiableList(streams);
    this.streamProducers = Collections.unmodifiableMap(streamProducers);
    this.streamConsumers = Collections.unmodifiableMap(streamConsumers);

    validate();
  }



  @Override
  public List<Processor> getAllProcessors() {
    return processors;
  }

  @Override
  public List<Processor> getProcessorsInStartOrder() {
    List<Processor> startOrder = new ArrayList<>();
    processorsLevelOrder().forEachRemaining(processor -> startOrder.add(processor));
    return startOrder;
  }

  @Override
  public List<PStream> getAllStreams() {
    return streams;
  }

  @Override
  public List<PStream> getManagedStreams() {
    List<PStream> managedStreams = new ArrayList<>(getIntermediateStreams());
    managedStreams.addAll(getSinkStreams());
    return managedStreams;
  }

  // TODO need these?
  // Processor getProcessor(String processorName)
  // Stream getStream(String streamName)

  @Override
  public List<PStream> getPrivateStreams() {
    return streams.stream()
        .filter(stream -> stream.getVisibility() == PStream.Visibility.PRIVATE)
        .collect(Collectors.toList());
  }

  @Override
  public List<PStream> getPublicStreams() {
    return streams.stream()
        .filter(stream -> stream.getVisibility() == PStream.Visibility.PUBLIC)
        .collect(Collectors.toList());
  }

  @Override
  public List<PStream> getSourceStreams() {
    return streams.stream()
        .filter(stream -> stream.getType() == PStream.Type.SOURCE)
        .collect(Collectors.toList());
  }

  @Override
  public List<PStream> getIntermediateStreams() {
    return streams.stream()
        .filter(stream -> stream.getType() == PStream.Type.INTERMEDIATE)
        .collect(Collectors.toList());
  }

  @Override
  public List<PStream> getSinkStreams() {
    return streams.stream()
        .filter(stream -> stream.getType() == PStream.Type.SINK)
        .collect(Collectors.toList());
  }

  @Override
  public List<Processor> getStreamProducers(PStream stream) {
    Collection<Processor> producers = streamProducers.get(stream);

    if (producers == null) {
      return Collections.emptyList();
    }

    return Collections.unmodifiableList(new ArrayList<>(producers));
  }

  @Override
  public List<Processor> getStreamConsumers(PStream stream) {
    Collection<Processor> consumers = streamConsumers.get(stream);

    if (consumers == null) {
      return Collections.emptyList();
    }

    return Collections.unmodifiableList(new ArrayList<>(consumers));
  }

  @Override
  public List<PStream> getProcessorOutputs(Processor proc) {
    return streamProducers.entrySet().stream()
        .filter(entry -> entry.getValue().contains(proc))
        .map(entry -> entry.getKey())
        .collect(Collectors.toList());
  }

  @Override
  public List<PStream> getProcessorInputs(Processor proc) {
    return streamConsumers.entrySet().stream()
        .filter(entry -> entry.getValue().contains(proc))
        .map(entry -> entry.getKey())
        .collect(Collectors.toList());
  }

  public Iterator<Processor> processorsLevelOrder() {

    Multimap<Integer, Processor> depthMap = HashMultimap.create();
    Map<Processor, Integer> processorDepths = new HashMap<>();
    for (Processor processor : getAllProcessors()) {
      depthMap.put(findDepth(processor, processorDepths, 0), processor);
    }

    List<Processor> permutation = new ArrayList<>();
    for (int i = 0; i < depthMap.keySet().size(); i++) {
      // If depthMap.get() returns null, it means there isn't >=1 processor at each level == wrong! ==> throw the NPE
      permutation.addAll(depthMap.get(i));
    }

    Preconditions.checkState(permutation.size() == getAllProcessors().size(),
        String.format("Iterator error. \nPermutation: %s\nAll processors: %s", permutation, getAllProcessors()));

    return permutation.iterator();
  }

  // TODO: compile a list of validation errors and print them all in one exception
  private void validate() {
    nullChecks();

    // Stream type validations
    validateSourceStreams();
    validateIntermediateStreams();
    validateSinkStreams();

    // Stream visibility validations
    validatePrivateStreams();

    // Composition
    validateStreamTypeComposition();
    validateStreamVisibilityComposition();

    // TODO each processor should consume at least one stream, otherwise why is it a stream processor?
    // foreach stream validate()
    // foreach processor validate()
    // TODO no duplicate streams or processors

    if (getAllProcessors().isEmpty()) {
      throw new IllegalStateException("A Pipeline must have at least one processor.");
    }


    // Detect cycles
    Iterator<Processor> processors = processorsLevelOrder();
    Set<Processor> visitedProcessors = new HashSet<>();
    while (processors.hasNext()) {
      Processor processor = processors.next();
      if (!visitedProcessors.add(processor)) {
        throw new IllegalStateException(String.format("Cycle detected at processor %s", processor));
      }
    }
  }

  /**
   * Finds the max distance from this processor to all source nodes.
   * @param processor
   * @return
   */
  private int findDepth(Processor processor, Map<Processor, Integer> processorDepths, int recursionCount) {
    if (recursionCount > getAllProcessors().size()) {
      throw new IllegalStateException(String.format("Cycle detected by recursing more than the processor count %d", recursionCount));
    }

    if (processorDepths.containsKey(processor)) {
      return processorDepths.get(processor);
    }

    Set<Processor> upstreamProcessors = getProcessorInputs(processor).stream()
                                          .flatMap(stream -> getStreamProducers(stream).stream())
                                          .collect(Collectors.toSet());
    int maxDepth = 0;
    for (Processor upstreamProcessor : upstreamProcessors) {
      maxDepth = Math.max(maxDepth, findDepth(upstreamProcessor, processorDepths, recursionCount + 1) + 1);
    }
    processorDepths.put(processor, maxDepth);
    return maxDepth;
  }

  private void nullChecks() {
    Preconditions.checkNotNull(processors);
    Preconditions.checkNotNull(streams);
    Preconditions.checkNotNull(streamProducers);
    Preconditions.checkNotNull(streamConsumers);
    for (Processor processor : processors) {
      Preconditions.checkNotNull(processor);
    }
    for (PStream stream : streams) {
      Preconditions.checkNotNull(stream);
    }
    for (Map.Entry<PStream, Collection<Processor>> entry : streamConsumers.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
      for (Processor processor : entry.getValue()) {
        Preconditions.checkNotNull(processor);
      }
    }
    for (Map.Entry<PStream, Collection<Processor>> entry : streamProducers.entrySet()) {
      Preconditions.checkNotNull(entry.getKey());
      Preconditions.checkNotNull(entry.getValue());
      for (Processor processor : entry.getValue()) {
        Preconditions.checkNotNull(processor);
      }
    }
  }

  private void validateSourceStreams() {
    if (getSourceStreams().isEmpty()) {
      throw new IllegalStateException("A Pipeline must have at least one source stream.");
    }

    for (PStream source : getSourceStreams()) {

      // Source streams should have at least 1 consumer
      List<Processor> consumers = getStreamConsumers(source);
      if (consumers.isEmpty()) {
        throw new IllegalArgumentException(String.format("SOURCE stream %s should have at least 1 consumer: %s", source, consumers.toString()));
      }

      // Source streams should have no producer
      List<Processor> producers = getStreamProducers(source);
      if (!producers.isEmpty()) {
        throw new IllegalArgumentException(String.format("SOURCE stream %s should not have producers: %s", source, producers.toString()));
      }

      // Source streams must be public
      if (source.getVisibility() != PStream.Visibility.PUBLIC) {
        throw new IllegalArgumentException(String.format("SOURCE stream %s should have PUBLIC visibility.", source));
      }
    }
  }

  private void validateIntermediateStreams() {

    for (PStream intermediate : getIntermediateStreams()) {

      // Intermediate streams should have at least 1 consumer
      List<Processor> consumers = getStreamConsumers(intermediate);
      if (consumers.isEmpty()) {
        throw new IllegalArgumentException(String.format("INTERMEDIATE stream %s should have at least 1 consumer: %s", intermediate, consumers.toString()));
      }

      // Intermediate streams should have at least 1 producer
      List<Processor> producers = getStreamProducers(intermediate);
      if (producers.isEmpty()) {
        throw new IllegalArgumentException(String.format("INTERMEDIATE stream %s should have at least 1 producer: %s", intermediate, producers.toString()));
      }
    }
  }

  private void validateSinkStreams() {

    for (PStream sink : getSinkStreams()) {

      // Sink streams should have no consumer (in the pipeline)
      List<Processor> consumers = getStreamConsumers(sink);
      if (!consumers.isEmpty()) {
        throw new IllegalArgumentException(String.format("SINK stream %s should not have consumers: %s", sink, consumers.toString()));
      }

      // Sink streams should have at least 1 producer
      List<Processor> producers = getStreamProducers(sink);
      if (producers.isEmpty()) {
        throw new IllegalArgumentException(String.format("SINK stream %s should have at least 1 producer: %s", sink, producers.toString()));
      }

      // Sink streams must be public
      if (sink.getVisibility() != PStream.Visibility.PUBLIC) {
        throw new IllegalArgumentException(String.format("SINK stream %s should have PUBLIC visibility.", sink));
      }
    }
  }

  private void validatePrivateStreams() {

    for (PStream privateStream : getPrivateStreams()) {

      // Private streams must be intermediate streams, never source or sink.
      // Public streams can be any type (source, intermediate, sink)
      if (privateStream.getType() != PStream.Type.INTERMEDIATE) {
        throw new IllegalArgumentException(String.format(
              "Consistency error with stream %s. Only INTERMEDIATE type streams can be PRIVATE.", privateStream));
      }
    }
  }

  private void validateStreamTypeComposition() {

    Set<PStream> allStreams = new HashSet<>(streams);
    List<PStream> sourceStreams = getSourceStreams();
    List<PStream> intermediateStreams = getIntermediateStreams();
    List<PStream> sinkStreams = getSinkStreams();

    // Source, intermediate, and sink streams should be mutually exclusive
    if (!(Collections.disjoint(sourceStreams, intermediateStreams) &&
          Collections.disjoint(sourceStreams, sinkStreams) &&
          Collections.disjoint(intermediateStreams, sinkStreams))) {
      throw new IllegalArgumentException(String.format(
            "Stream types were not mutually exclusive. \nSource: %s\nIntermediate: %s\nSink: %s",
            sourceStreams, intermediateStreams, sinkStreams));
    }

    // Source, intermediate, and sink streams should together represent all streams
    int totalTypes = sourceStreams.size() + intermediateStreams.size() + sinkStreams.size();
    if (!(totalTypes == streams.size() &&
          allStreams.containsAll(sourceStreams) &&
          allStreams.containsAll(intermediateStreams) &&
          allStreams.containsAll(sinkStreams))) {
      throw new IllegalArgumentException(String.format(
            "Inconsistent stream types. \nAll: %s\nSource: %s\nIntermediate: %s\nSink: %s",
            streams, sourceStreams, intermediateStreams, sinkStreams));
    }
  }

  private void validateStreamVisibilityComposition() {

    Set<PStream> allStreams = new HashSet<>(streams);
    List<PStream> privateStreams = getPrivateStreams();
    List<PStream> publicStreams = getPublicStreams();

    // Private and public streams should be mutually exclusive sets
    if (!Collections.disjoint(privateStreams, publicStreams)) {
      throw new IllegalArgumentException(String.format(
            "Stream visibilities were not mutually exclusive. \nPrivate: %s\nPublic: %s",
            privateStreams, publicStreams));
    }

    // Private and public streams should together represent all streams
    int totalVisibilities = privateStreams.size() + publicStreams.size();
    if (!(totalVisibilities == streams.size() &&
        allStreams.containsAll(privateStreams) &&
        allStreams.containsAll(publicStreams))) {
      throw new IllegalArgumentException(String.format(
            "Inconsistent stream visibilities. \nAll: %s\nPrivate: %s\nPublic: %s",
            streams, privateStreams, publicStreams));
    }
  }
}
