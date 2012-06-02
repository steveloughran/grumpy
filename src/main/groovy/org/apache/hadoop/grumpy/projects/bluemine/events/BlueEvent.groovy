package org.apache.hadoop.grumpy.projects.bluemine.events

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.WritableComparable

/**
 * Events are parseable and writeable
 */
class BlueEvent implements Writable, WritableComparable, Cloneable {

  String device
  String gate
  String name
  Date datestamp

  long duration

  /**
   * the empty date
   */
  static final Epoch = new Date(0)

  BlueEvent() {
    reset()
  }

  /**
   * Reset the event for re-use; sets everything to non null
   * empty values
   */
  void reset() {
    device = ""
    gate = ""
    name = ""
    datestamp = Epoch
    duration = 0
  }

  boolean getValid() {
    device != null && !device.empty
  }

  /**
   * This generates an extended value
   * @return
   */
  @Override
  String toString() {
    return "${gate},${device},$duration,${datestamp},${name}"
  }

  @Override
  void write(DataOutput out) {
    out.writeUTF(device)
    out.writeUTF(denullify(gate))
    out.writeUTF(denullify(name))
    out.writeLong(datestamp ? datestamp.time : 0)
    out.writeLong(duration)
  }

  String denullify(String s) { s ?: "" }

  @Override
  void readFields(DataInput src) throws IOException {
    device = src.readUTF()
    gate = src.readUTF()
    name = src.readUTF()
    datestamp = new Date(src.readLong());
    duration = src.readLong()
  }

  long getStarttime() {
    datestamp.time
  }

  long getEndtime() {
    duration + datestamp.time
  }

  /**
   * Merge two events to build up a combined duration. If the later event actually came in before
   * then the merge is done in the other order.
   * @param laterEvent the event that should have come later
   * @return the duration
   */
  long merge(BlueEvent laterEvent) {
    //sanity checks -events have timestamps
    assert starttime != 0
    assert laterEvent.starttime != 0
    duration = laterEvent.endtime - starttime
    if (duration < 0) {
      //actually, the events are out of order
      duration = -duration
    }
    duration
  }

  @Override
  int compareTo(Object o) {
    BlueEvent that = (BlueEvent) o;
    if (!device) return -1
    return device.compareTo(that.device)
  }

  @Override
  protected BlueEvent clone() {
    return (BlueEvent) super.clone()
  }


  public boolean overlaps(BlueEvent other) {
    long s1 = starttime
    long e1 = endtime
    long s2 = other.starttime
    long e2 = other.endtime
    !((e1 < s2) || (e2 < s1))
  }

}
