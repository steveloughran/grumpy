package org.apache.hadoop.grumpy.projects.bluemine.mr

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent

/**
 * Map to a day of the week. This mapper supports an offset day, defined by
 * {@link org.apache.hadoop.grumpy.projects.bluemine.BluemineOptions#OPT_HOUR_DAY_STARTS}; this allows the day to begin
 * at, say 3cd for 3 am; all events before that are deemed to belong to the previous day.
 *
 */
class MapToDayOfWeek extends MapToHour {

    int startHour;

    @Override
    protected void setup(Mapper.Context context) {
        super.setup(context)
        startHour = context.configuration.getInt(OPT_HOUR_DAY_STARTS, 0)
    }

    @Override
    String selectOutputKey(BlueEvent event, Mapper.Context context) {
        Date date = event.datestamp
        int hour = date.hours
        int day = date.day
        if (hour < startHour) day = previousDayOfWeek(day)
        return day.toString()
    }

    /**
     * convert to the previous day of the week. 6->5, 0 -> 7, etc.
     * @param weekday
     * @return the new value
     */
    int previousDayOfWeek(int weekday) {
        (weekday + 6) % 7
    }

}
