package org.apache.samza.pipeline;

/**
 * Could be renamed to ProcessorSpec
 *
 * This class is immutable.
 */
public class Processor {
  private final String name;
  // The task class corresponding to the task.class property in the config.
//  private final Class<? extends StreamTask> clazz;

  // TODO alt; instead of a class, we could have a class name and URL to a tarball so they don't have to be packaged together

//  public Processor(Class<? extends StreamTask> processorClass) {
//    clazz = processorClass;
//  }

  public Processor(String name) {
    // TODO null checks and validation
    this.name = name;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Processor)) {
      return false;
    }
    return name.equals(((Processor) obj).name);
  }

  @Override
  public String toString() {
    // TODO more precise string
    return name;
  }
}
