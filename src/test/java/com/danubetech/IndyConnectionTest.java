package com.danubetech;

import com.danubetech.libindy.IndyConnection;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IndyConnectionTest {

    @Test
    public void testDIDDocument() throws Exception {

        assertEquals(IndyConnection.getNetwork(""), "_");
        assertEquals(IndyConnection.getNetwork("idunion:"), "idunion");
        assertEquals(IndyConnection.getDidNetworkPrefix("_"), "");
        assertEquals(IndyConnection.getDidNetworkPrefix("idunion"), "idunion:");
    }
}
