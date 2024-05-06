/*
 * Copyright 2023 Google LLC All Rights Reserved
 *
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

package com.google.zetasql;

/** Only public to make it possible to copy the SimpleCatalog */
public class SimpleCatalogUtil {
  public static SimpleCatalog copyCatalog(SimpleCatalog sourceCatalog) {
    // Simply serializes and deserializes the source catalog to create a copy.
    // This is the most reliable way of creating a copy of a SimpleCatalog,
    // as the SimpleCatalog's public interface does not expose enough of the internal
    // structures to create an accurate copy.
    FileDescriptorSetsBuilder fileDescriptorSetsBuilder = new FileDescriptorSetsBuilder();
    SimpleCatalogProtos.SimpleCatalogProto serialized =
        sourceCatalog.serialize(fileDescriptorSetsBuilder);
    return SimpleCatalog.deserialize(serialized, fileDescriptorSetsBuilder.getDescriptorPools());
  }
}
