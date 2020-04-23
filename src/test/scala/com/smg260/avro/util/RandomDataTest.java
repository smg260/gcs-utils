package com.smg260.avro.util;

import com.bouncex.platformx.common.avro.EspSetting;

import static org.junit.jupiter.api.Assertions.*;

class RandomDataTest {

    @org.junit.jupiter.api.Test
    void setAllowNulls() {
        RandomData<EspSetting> espSettings = new RandomData<>(EspSetting.getClassSchema(), 1);
        espSettings.setAllowNulls(false);

        espSettings.iterator().forEachRemaining(System.out::println);
    }
}