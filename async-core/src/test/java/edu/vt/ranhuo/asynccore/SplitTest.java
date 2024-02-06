package edu.vt.ranhuo.asynccore;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static edu.vt.ranhuo.asynccore.enums.CommonConstants.HASH_VALUE_SPLIT_ESCAPE;
import static edu.vt.ranhuo.asynccore.enums.CommonConstants.REDIS_SPLIT;

@Slf4j
public class SplitTest {
    @Test
    public void split() {
        String hashValue = "task_second^codewave^task_first";
        log.info(String.join(REDIS_SPLIT, deleteSplit(hashValue)));
    }

    public List<String> deleteSplit(String value) {
        Objects.requireNonNull(value);
        return new ArrayList<>(Arrays.asList(value.split(HASH_VALUE_SPLIT_ESCAPE)));
    }
}