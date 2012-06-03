package org.apache.hadoop.grumpy.projects.bluemine.mr.test

import org.apache.hadoop.grumpy.projects.bluemine.BluemineOptions
import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent
import org.apache.hadoop.grumpy.projects.bluemine.events.EventParser
import org.apache.hadoop.grumpy.projects.bluemine.events.EventWindow
import org.apache.hadoop.grumpy.projects.bluemine.mr.testtools.BluemineTestBase
import org.junit.Test

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
class EventWindowTest extends BluemineTestBase {

  EventParser parser = new EventParser()
  EventWindow window
  BlueEvent s1
  BlueEvent no_name
  BlueEvent s2
  BlueEvent s3, s4

  @Override
  protected void setUp() {
    super.setUp()
    window = new EventWindow()
    s1 = parser.parse(SMILEY)
    no_name = parser.parse(NO_NAME)
    s2 = parser.parse(SMILEY2)
    s3 = parser.parse(SMILEY3)
    s4 = parser.parse(SMILEY4)
  }

  @Test
  public void testDefaultDuration() {
    long duration = s1.duration
    assert duration == BluemineOptions.INITIAL_DURATION
  }

  @Test
  public void testMergeDuration() {
    long d1 = s1.duration
    long e1 = s1.endtime
    long d2 = s2.duration
    long e2 = s2.endtime
    s1.merge(s2)
    assert s1.duration > d1
    assert s1.endtime == s2.endtime
  }

  @Test
  public void testInsertCloning() {
    BlueEvent sclone = window.insert(s1)
    assert !(sclone.is(s1))
  }

  @Test
  public void testWindowSearch() {
    window.addClone(s1)
    assert 1 == window.size
    assert window.findMatchingEventInWindow(s1)
    def all = window.findAll {it -> true}
    assert 1 == all.size()

    //look for in/out searches
    assert window.findMatchingEventInWindow(s2)
    assert window.findMatchingEventInWindow(s3)
    assert window.findMatchingEventInWindow(no_name) == null
    //check the gate is in the test
    BlueEvent s4 = (BlueEvent) s3.clone()
    s4.gate = "new-gate"
    assert !window.findMatchingEventInWindow(s4)
  }

  @Test
  public void testWindowInsert() {
    window.insert(s1)
    assert 1 == window.size
    BlueEvent inserted = window.insert(s2)
    assert 1 == window.size
    assert s1.starttime == inserted.starttime
    assert s2.endtime == inserted.endtime
  }


  @Test
  public void testOverlap() {
    window.insert(s1)
    assert 1 == window.size
    BlueEvent inserted = window.insert(s2)
    assert 1 == window.size
    assert s1.starttime == inserted.starttime
    assert s2.endtime == inserted.endtime
    assert s1.overlaps(inserted)
    assert s2.overlaps(inserted)
    assert inserted.overlaps(no_name)
    assert !inserted.overlaps(s4)
  }


  @Test
  public void testPurge() {
    window.insert(s1)
    window.insert(no_name)
    window.insert(s2)
    log.info("Window = $window.window")
    List<BlueEvent> purged = window.purgeExpired(s3)
    log.info("Purged = $purged")
    assert purged.size() == 1
    assert purged.find { it.device == no_name.device}
    assert window.findMatchingEventInWindow(no_name) == null
    assert window.findMatchingEventInWindow(s1)
  }
}
