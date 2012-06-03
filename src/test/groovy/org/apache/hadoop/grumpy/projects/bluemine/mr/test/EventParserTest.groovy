package org.apache.hadoop.grumpy.projects.bluemine.mr.test

import org.apache.hadoop.grumpy.projects.bluemine.events.BlueEvent
import org.apache.hadoop.grumpy.projects.bluemine.events.EventParser
import org.apache.hadoop.grumpy.projects.bluemine.mr.testtools.BluemineTestBase
import org.junit.Test

/**
 *
 */
class EventParserTest extends BluemineTestBase {


  @Test
  public void testParser() throws Throwable {
    EventParser parser = new EventParser()

    LINES.each { line ->
      LOG.info("Parsing : $line")
      BlueEvent event = parser.parse(line)
      LOG.info("Parsed: $event")
    }
  }

  @Test
  public void testTrim() throws Throwable {
    EventParser parser = new EventParser()
    assertEquals("trimmed", parser.trim(" trimmed "));
    assertEquals("", parser.trim("  "));
    assertEquals(null, parser.trim(null));
  }

  @Test
  public void testDateOff() throws Throwable {
    EventParser parser = new EventParser()
    parser.parseDatestamp = false
    LINES.each { line ->
      LOG.info("Parsing : $line")
      BlueEvent event = parser.parse(line)
      assertNull("Expected no date in " + event.toString(), event.datestamp);
      LOG.info("Parsed: $event")
    }
  }

  @Test
  public void testTroublesomeName() throws Throwable {
    EventParser parser = new EventParser()
    BlueEvent event = parser.parse(COMMA1)
    assertNotNull("Null name from $COMMA1 -> $event", event.name)
    assertEquals(",) Where am i?", event.name)
  }

  @Test
  public void testTroublesomeName2() throws Throwable {
    EventParser parser = new EventParser()
    BlueEvent event = parser.parse(COMMA2)
    assertNotNull("Null name from $COMMA2 -> $event", event.name)
    assertEquals(")\"\', Where am i?", event.name)
  }

  public void testParsedEventCloneable() throws Throwable {
    EventParser parser = new EventParser()
    BlueEvent event = parser.parse(NO_NAME)
    event.clone()
  }

  public void testParseRoundTripNoName() throws Throwable {
    assertRoundTrip(NO_NAME)
  }

  def assertRoundTrip(String original) {
    EventParser parser = new EventParser()
    BlueEvent event = parser.parse(original)
    String csv = parser.convertToCSV(event);
    log.info("${original} => {$csv}")
    BlueEvent ev2 = parser.parse(csv)
    assertEventsEqual(event, ev2)
  }

  def assertEventsEqual(BlueEvent expected, BlueEvent actual) {
    assert actual.gate == expected.gate
    assert actual.device == expected.device
    assert actual.datestamp == expected.datestamp
    assert actual.name == expected.name
    assert actual.duration == expected.duration
  }

  public void testParseRoundTripComma1() throws Throwable {
    assertRoundTrip(COMMA1);
  }

  public void testParseRoundTripComma2() throws Throwable {
    assertRoundTrip(COMMA2);
  }

  public void testParseRoundTripDuration() throws Throwable {
    EventParser parser = new EventParser()
    BlueEvent event = parser.parse(VKLAPTOP)
    event.duration = 4096
    String csv = parser.convertToCSV(event)
    log.info("${VKLAPTOP} => {$csv}")
    BlueEvent ev2 = parser.parse(csv)
    assertEventsEqual(event, ev2);
    log.info("Reparsed = $ev2")
  }

}
