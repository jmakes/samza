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

package org.apache.samza.pipeline.stream;

import org.apache.samza.pipeline.api.StreamManager;
import org.apache.samza.system.SystemAdmin;


// TODO DODODODODODODODO Test with a kafka dev cluster so as to not pollute other clusters with test topics.
public class KafkaStreamManager implements StreamManager {
  private final SystemAdmin admin;
      // TODO should the KafkaStreamManager know about Pipeline or just start individual PStreams? Perhaps even with pipeline it should be stateless.

  public KafkaStreamManager(SystemAdmin admin) {
    this.admin = admin;
  }

  @Override
  public void createOrUpdateStream(PStream stream) {
    // todo exception if any failure. We should not continue with running a pipeline with misconfigured streams
    admin.createChangelogStream(stream.getSystemStream().getStream(), stream.getPartitionCount());
  }

  public void configureAllStreams() {

    // todo
  }
}