package com.ws.ogre;

import org.junit.Before;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

public abstract class AbstractBaseTest {
    @Before
    public void setUp() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    protected <T> Map<String, T> asMap(T... theParams) {
        Map<String, T> aMap = new LinkedHashMap<>();

        for (int i = 0; i < theParams.length; i += 2) {
            aMap.put(theParams[i].toString(), theParams[i + 1]);
        }

        return aMap;
    }
}
