package org.apache.hadoop.grumpy.projects.bluemine.mr

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

import groovy.util.logging.Commons
import org.apache.hadoop.grumpy.projects.bluemine.BluemineOptions
import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent
import org.apache.hadoop.grumpy.projects.bluemine.events.EventParser

/**
 * Be aware that Groovy's semantics of inner classes is very different from Java's (esp. when it comes
 * to access to outer fields from the inner class; this can cause confusion in the compilers and IDEs
 * when it comes to referring to the nested Context object
 */
@Commons
abstract class AbstractBlueMapper extends Mapper<LongWritable, Text, Text, BlueEvent>
implements BluemineOptions {

    public static final String BLUEMINE_COUNTERS = "bluemine"
    public static final String COUNTER_INPUT_ERRORS = "input errors"
    public static final String COUNTER_INVALID_EVENTS = "invalidEvents"
    protected EventParser parser = new EventParser()
    protected Text outputKey = new Text()
    protected String keyString
    private BlueEvent blueEvent = new BlueEvent()

    protected Text inputLine

    /**
     * Parse and emit events
     * @param key line #
     * @param value raw line
     * @param context ctx
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) {
        inputLine = value
        try {
            parser.parse(blueEvent, value, "at offset " + key)
        } catch (IOException ioe) {
            log.warn(ioe);
            incCounter(context, COUNTER_INPUT_ERRORS)
            return;
        }
        if (!blueEvent.valid) {
            log.warn("Invalid event from ${value.toString()} : $blueEvent")
            incCounter(context, COUNTER_INVALID_EVENTS)
        }
        process(context, key, value, blueEvent)
    }

    protected void incCounter(Mapper.Context context, String name) {
        context.getCounter(BLUEMINE_COUNTERS, name).increment(1)
    }

    /**
     * Base process operation
     * @param context context
     * @param lineNo line number
     * @param line ext itself
     * @param event the already parsed event
     */
    void process(Mapper.Context context, LongWritable lineNo, Text line, BlueEvent event) {
        keyString = selectOutputKey(event, context)
        if (keyString == null) {
            log.warn("Null output key parsing \"" + line + "\"");
            context.getCounter(BLUEMINE_COUNTERS, "key errors").increment(1)
        } else {
            outputKey.set(keyString)
            process(lineNo, context, event)
        }
    }

    /**
     * When invoked , event is the current event, outputKey is set to the Text to write
     * @param key
     * @param context
     */
    void process(LongWritable key, Mapper.Context context, BlueEvent event) {
        context.write(outputKey, event)
    }

    /**
     * Select the output key
     * @param event
     * @param context
     * @return
     */
    String selectOutputKey(BlueEvent event, Mapper.Context context) {
        return event.device
    }

}