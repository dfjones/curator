package org.apache.curator.framework.recipes.leader;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.utils.CloseableUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestIssue62 extends BaseClassForTests {

  @Test
  public void testInterrupted() throws Exception
  {
    LeaderSelector      selectorOne = null;
    LeaderSelector      selectorTwo = null;
    CuratorFramework    client = CuratorFrameworkFactory.builder().connectString(server.getConnectString()).retryPolicy(new RetryOneTime(1)).sessionTimeoutMs(
        1000).build();
    try
    {
      client.start();

      final CountDownLatch latch = new CountDownLatch(2);
      final AtomicReference<LeaderSelector> selectorRef = new AtomicReference<LeaderSelector>(null);
      LeaderSelectorListener listener = new LeaderSelectorListener()
      {
        @Override
        public void takeLeadership(CuratorFramework client) throws Exception
        {
          latch.countDown();
          if (latch.getCount() >= 1) {
            selectorRef.get().interrupt = true;
          }
        }

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) { }
      };
      selectorOne = new LeaderSelector(client, "/leader", listener);
      selectorOne.autoRequeue();
      selectorRef.set(selectorOne);
      selectorOne.start();

      Thread.sleep(1000);
      selectorTwo = new LeaderSelector(client, "/leader", listener);
      selectorTwo.autoRequeue();
      selectorTwo.start();

      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
    }
    finally
    {
      CloseableUtils.closeQuietly(selectorOne);
      CloseableUtils.closeQuietly(selectorTwo);
      CloseableUtils.closeQuietly(client);
    }
  }
}
