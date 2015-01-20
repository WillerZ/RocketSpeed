package org.rocketspeed.util;

import org.rocketspeed.Status;

public class StatusMonoidFirst {

  private Status status = Status.OK;

  public synchronized void append(Status status) {
    if (this.status.isOk()) {
      this.status = status;
    }
  }

  public synchronized void checkExceptions() throws Exception {
    status.checkExceptions();
  }
}
