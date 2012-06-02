package org.apache.hadoop.grumpy.projects.bluemine.mr

import org.apache.hadoop.mapreduce.Mapper

import groovy.util.logging.Commons
import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent
import org.apache.hadoop.grumpy.projects.bluemine.utils.BluemineException

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
@Commons
class FilterDeviceByYear extends MapEmitDevice {

    int year;

    @Override
    protected void setup(Mapper.Context context) {
        super.setup(context)
        year = context.configuration.getInt(FILTER_YEAR, 0)
        if (year == 0) {
            throw new BluemineException("Unset configuration option $FILTER_YEAR")
        }
        year -= 1900
        log.info("Adjusted filter year is " + year)
    }

    @Override
    protected boolean shouldEmit(BlueEvent event) {
        return event.datestamp.year == year
    }
}
