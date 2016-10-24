package org.apache.samza.pipeline.api;

import org.apache.samza.pipeline.stream.PStream;


public interface StreamManager {
  void createOrUpdateStream(PStream stream);

}
