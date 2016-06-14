package org.apache.myriad.scheduler.fgs;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.myriad.TestObjectFactory;
import org.junit.Test;

/**
 * Unit tests for NodeStore class
 */
public class NodeStoreTest {
  NodeStore store = new NodeStore();
  SchedulerNode sNode = TestObjectFactory.getSchedulerNode(NodeId.newInstance("localhost",8888), 2, 4096);

  @Test
  public void testAddNode() throws Exception {
    store.add(sNode);
    assertTrue(store.isPresent("localhost"));
    assertNotNull(store.getNode("localhost"));
  }

  @Test
  public void testRemoveNode() throws Exception {
    if(!store.isPresent("localhost")) {
      store.add(sNode);
    }
    store.remove("localhost");
    assertFalse(store.isPresent("localhost"));
    assertNull(store.getNode("localhost"));
  }
}