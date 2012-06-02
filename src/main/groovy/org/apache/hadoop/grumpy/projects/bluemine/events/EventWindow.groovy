package org.apache.hadoop.grumpy.projects.bluemine.events

import org.apache.hadoop.grumpy.projects.bluemine.BluemineOptions

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class EventWindow implements Iterable<BlueEvent> {
  long windowDuration = BluemineOptions.DEBOUNCE_WINDOW
  long insertDuration = BluemineOptions.INITIAL_DURATION

  //use a linked list here on account of the many deletions off the head that are planned.
  List<BlueEvent> window = []

  BlueEvent findMatchingEventInWindow(BlueEvent event) {
    String devID = event.device
    String gate = event.gate
    BlueEvent found = window.find { it.device == devID && it.gate == gate }
    return found
  }

  BlueEvent addClone(BlueEvent event) {
    BlueEvent cloned = event.clone()
    window.add(cloned);
    cloned
  }


  Iterator<BlueEvent> iterator() {window.listIterator()}

  List<BlueEvent> findAll(Closure closure) {
    window.findAll(closure)
  }

  void removeAll(Collection c) {
    window.removeAll(c)
  }

  int getSize() {
    return window.size()
  }

  BlueEvent insert(BlueEvent event) {
    if (!event.duration) {
      event.duration = insertDuration
    }
    BlueEvent inWindow = findMatchingEventInWindow(event)
    if (inWindow) {
      //event in the window
      //add its duration to the current event
      inWindow.merge(event)
    } else {
      //no ongoing event, add a clone of it (remember, events get re-used, so a clone is mandatory)
      inWindow = addClone(event)
    }
    inWindow
  }

  /**
   //go through the window and extract those that are out of range, that is their end time falls
   // before the window duration of the next event
   * @param now
   * @return
   */
  List<BlueEvent> purgeExpired(BlueEvent now) {

    long closingtime = now.endtime - windowDuration

    List<BlueEvent> expired = window.findAll { !it.overlaps(now) }
    window.removeAll(expired)
    expired
  }
}
