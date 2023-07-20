package com.google.zetasql.toolkit.usage;

import static org.junit.jupiter.api.Assertions.*;

import com.google.api.gax.rpc.HeaderProvider;
import org.junit.jupiter.api.Test;

public class UsageTrackingTest {

  @Test
  public void testRevisionIsSet() {
    assertNotNull(UsageTracking.CURRENT_REVISION, "Project revision not set");
    assertNotEquals(
        "UNSET", UsageTracking.CURRENT_REVISION, "Project revision not set");
  }

  @Test
  public void testUserAgentIsSet() {
    HeaderProvider headerProvider = UsageTracking.HEADER_PROVIDER;
    String userAgentValue = headerProvider.getHeaders().get("user-agent");
    assertNotNull(userAgentValue, "User agent for API calls not set");
  }

}
