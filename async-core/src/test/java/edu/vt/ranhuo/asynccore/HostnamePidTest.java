package edu.vt.ranhuo.asynccore;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
@Slf4j
public class HostnamePidTest {
    @Test
    public void hostnamePid() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        log.info(runtimeMXBean.getName());
    }
}
