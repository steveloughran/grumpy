package org.apache.hadoop.grumpy.projects.bluemine.mr

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapreduce.Mapper

import groovy.util.logging.Commons
import org.apache.hadoop.grumpy.projects.bluemine.BluemineOptions
import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent
import org.apache.hadoop.grumpy.projects.bluemine.events.EventWindow

/**
 * sliding window debouncing of bluetooth events
 */
@Commons
class DebounceMap extends AbstractBlueMapper {

    private EventWindow window
    //max time for an extended event before a warning is generated
    private static final long WARN_DURATION = 4 * 60 * 60 * 1000L
    private static final String COUNTER_OVERLONG_EVENT = "duration too long"
    private static final String COUNTER_INVALID_DURATION = "duration negative"

    @Override
    protected void setup(final Mapper.Context context) {
        super.setup(context)
        window = new EventWindow(windowDuration: BluemineOptions.DEBOUNCE_WINDOW)
    }
/**
 * Output key is (year, month, day,gate). For any specific day, the ordering is handled by the reducer
 * @param event event date
 * @param context context
 * @return
 */
    @Override
    String selectOutputKey(final BlueEvent event, final Mapper.Context context) {
        return dateKey(event)
    }

    protected String dateKey(BlueEvent event) {
        Date date = event.datestamp
        return date.year + "-" + date.month + "-" + date.day + "@" + event.gate
    }

    @Override
    void process(LongWritable key, Mapper.Context context, BlueEvent event) {
        log.debug("Event $event")
        BlueEvent ev2 = window.insert(event)
        log.debug("Inserted $event")
        List<BlueEvent> expired = window.purgeExpired(event)
        if (!expired.empty) {
            log.debug("${expired.size()} events leaving window: $expired")
        }
        expired.each { evt ->
            emit(context, evt)
        }
    }

    @Override
    protected void cleanup(final Mapper.Context context) {
        //end of run, emit everything left in the window
        log.debug("Cleanup - $window.size events to emit")
        window.each { event ->
            log.debug("Emitting $event")
            emit(context, event)
        }

        super.cleanup(context)
    }

    protected void emit(Mapper.Context context, BlueEvent event) {
        long d = event.duration;
        if (d>WARN_DURATION) {
            incCounter(context, COUNTER_OVERLONG_EVENT)
        } else if (d<0) {
            incCounter(context, COUNTER_INVALID_DURATION)
        }
        String key = dateKey(event)
        outputKey.set(key)
        context.write(outputKey, event)
    }


}
