package org.apache.samza.pipeline.stream;

import org.apache.samza.system.SystemStream;
import org.apache.samza.pipeline.api.KeyExtractor;


/**
 * Full specification of a stream s.t. it can be used to determine whether rekey or repartition is necessary.
 *
 * PStream == parallel stream, processor stream
 *
 * Could be renamed to StreamSpec
 *
 * This could be a base class for intermediate streams, source streams, and sink streams
 * Maybe even the notion of a public stream which requires a name, vs a private stream which auto-generates the name.
 *
 * This class is immutable.
 */
public class PStream {
  private final SystemStream systemStream;
  private final int partitionCount;
  private final KeyExtractor keyExtractor;
  private final Visibility visibility;
  private final Type type;

  /**
   * Public streams can be read/written by parties outside the pipeline.
   *
   * Private streams are intended to be used only by the pipeline. This designation allows us
   * to provide additional features like enabling "smart retention" which allows the stream to
   * expire messages as soon as they are consumed, or auto-generating topics.
   */
  public enum Visibility {
    PUBLIC,
    PRIVATE
  }

  public enum Type {
    SOURCE,
    INTERMEDIATE,
    SINK
  }

  public static PStream fromTemplate(PStream template, PStream.Type type) {
    return new PStream(template.systemStream, template.partitionCount, template.keyExtractor, type, template.visibility);
  }

  public static PStream fromTemplate(PStream template, PStream.Visibility visibility) {
    return new PStream(template.systemStream, template.partitionCount, template.keyExtractor, template.type, visibility);
  }

  public PStream(SystemStream systemStream, int partitionCount, KeyExtractor keyExtractor) {
    this(systemStream, partitionCount, keyExtractor, Type.INTERMEDIATE, Visibility.PRIVATE);
  }

  public PStream(SystemStream systemStream, int partitionCount, KeyExtractor keyExtractor, Type type, Visibility visibility) {
    // TODO null checks and validation
    this.systemStream = systemStream;
    this.partitionCount = partitionCount;
    this.keyExtractor = keyExtractor;
    this.visibility = visibility;
    this.type = type;
  }

  public SystemStream getSystemStream() {
    return systemStream;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public KeyExtractor getKeyExtractor() {
    return keyExtractor;
  }

  public Visibility getVisibility() {
    return visibility;
  }

  public Type getType() {
    return type;
  }

  @Override
  public int hashCode() {
    return systemStream.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof PStream)) {
      return false;
    }
    return systemStream.equals(((PStream) obj).systemStream);
  }

  @Override
  public String toString() {
    // TODO better tostring
    return String.format("{%s}", systemStream.toString());
  }

}
