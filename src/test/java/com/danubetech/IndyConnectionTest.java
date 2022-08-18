package com.danubetech;

import com.danubetech.libindy.IndyConnection;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IndyConnectionTest {

    @Test
    public void testIndyConnectionNetwork() throws Exception {

        assertEquals(IndyConnection.getNetwork(""), "_");
        assertEquals(IndyConnection.getNetwork("dummy:"), "dummy");
        assertEquals(IndyConnection.getNetwork("dummy:test:"), "dummy:test");
        assertEquals(IndyConnection.getDidNetworkPrefix("_"), "");
        assertEquals(IndyConnection.getDidNetworkPrefix("dummy"), "dummy:");
        assertEquals(IndyConnection.getDidNetworkPrefix("dummy:test"), "dummy:test:");
    }
}
