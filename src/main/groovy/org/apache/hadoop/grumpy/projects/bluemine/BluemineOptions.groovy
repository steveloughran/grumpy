/*
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

package org.apache.hadoop.grumpy.projects.bluemine

public interface BluemineOptions {

  String OPT_HOUR_DAY_STARTS = 'bluemine.hour.day.starts'

  String OPT_DAY_WEEK_STARTS = 'bluemine.day.week.starts'

  String DEFAULT_JOB_TRACKER = 'localhost:54311'

  String DEFAULT_FS = 'hdfs://localhost:54310'

  int DEBOUNCE_WINDOW = 60000

  int INITIAL_DURATION = 30000
  public String FILTER_YEAR = 'bluemine.filter.year'

  String MAPREDUCE_OUTPUTFORMAT_CLASS = 'mapreduce.outputformat.class'
}