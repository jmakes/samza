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

/**
 * Could be renamed to ProcessorSpec
 *
 * This class is immutable.
 */
public class Processor {
  private final String name;
  // The task class corresponding to the task.class property in the config.
//  private final Class<? extends StreamTask> clazz;

  // TODO alt; instead of a class, we could just have an instance

//  public Processor(Class<? extends StreamTask> processorClass) {
//    clazz = processorClass;
//  }

  public Processor(String name) {
    // TODO null checks and validation
    this.name = name;
  }

  public String getName() {
    return name;
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
