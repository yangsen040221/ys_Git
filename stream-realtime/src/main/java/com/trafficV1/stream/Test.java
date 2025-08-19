package com.trafficV1.stream;

import com.github.houbb.sensitive.word.core.SensitiveWord;
import com.github.houbb.sensitive.word.core.SensitiveWordHelper;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.RedisLuaUtils;
import lombok.SneakyThrows;
import org.apache.derby.catalog.UUID;

/**
 * @Package com.stream.Test
 * @Author zhou.han
 * @Date 2024/12/29 22:45
 * @description:
 */
public class Test {
    @SneakyThrows
    public static void main(String[] args) {
        String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
        System.err.println(kafka_botstrap_servers);
    }

}
