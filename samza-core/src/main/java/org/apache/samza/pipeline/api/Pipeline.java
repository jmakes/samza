package org.apache.samza.pipeline.api;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.samza.pipeline.Processor;
import org.apache.samza.pipeline.stream.PStream;


// TODO: This interface may not be needed. Depends on whether there is any contract we want to enforce.
public interface Pipeline {
  List<Processor> getAllProcessors();
  List<Processor> getProcessorsInStartOrder();

  List<PStream> getAllStreams();
  List<PStream> getManagedStreams();
  List<PStream> getPrivateStreams();
  List<PStream> getPublicStreams();
  List<PStream> getSourceStreams();
  List<PStream> getIntermediateStreams();
  List<PStream> getSinkStreams();

  /**
   *
   * @param stream
   * @return the list of producers in this pipeline for the specified stream. Never null.
   */
  List<Processor> getStreamProducers(PStream stream);

  /**
   *
   * @param stream
   * @return the list of consumers in this pipeline for the specified stream. Never null.
   */
  List<Processor> getStreamConsumers(PStream stream);
  List<PStream> getProcessorOutputs(Processor proc);
  List<PStream> getProcessorInputs(Processor proc);
}
