package org.rocketspeed;

import org.junit.Test;

public class ProxyBindingsLinkTest {

  @Test
  public void testLinking() throws Exception {
    Configuration config = new Configuration();
    Proxy proxy = new Proxy(LogLevel.DEBUG_LEVEL, config, 1);
    // The proxy will not be started, but will be created, which involves JNI call.
    // This way we check that Java succeeded linking JNI bindings.
    proxy.close();
  }
}
