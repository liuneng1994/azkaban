/*
 * Copyright 2014 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.utils;

import org.junit.Assert;
import org.junit.Test;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.TriggerBuilder;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Test class for azkaban.utils.Utils
 */
public class UtilsTest {

    /* Test negative port case */
    @Test
    public void testNegativePort() {
        Assert.assertFalse(Utils.isValidPort(-1));
        Assert.assertFalse(Utils.isValidPort(-10));
    }

    /* Test zero port case */
    @Test
    public void testZeroPort() {
        Assert.assertFalse(Utils.isValidPort(0));
    }

    /* Test port beyond limit */
    @Test
    public void testOverflowPort() {
        Assert.assertFalse(Utils.isValidPort(70000));
        Assert.assertFalse(Utils.isValidPort(65536));
    }

    /* Test happy isValidPort case*/
    @Test
    public void testValidPort() {
        Assert.assertTrue(Utils.isValidPort(1023));
        Assert.assertTrue(Utils.isValidPort(10000));
        Assert.assertTrue(Utils.isValidPort(3030));
        Assert.assertTrue(Utils.isValidPort(1045));
    }

    @Test
    public void test() {
        try {
            Map<String, String> a = (Map<String, String>) JSONUtils.parseJSONFromString("{\"a\":1}");
            System.out.println(a.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1() {
        //上次执行时间
        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String[] times = new String[2];
        CronTrigger trigger = TriggerBuilder.newTrigger().withIdentity("Caclulate Date").withSchedule(CronScheduleBuilder.cronSchedule("0 /3 * ? * *")).build();
        Date time0 = trigger.getStartTime();
        Date time1 = trigger.getFireTimeAfter(time0);
        Date time2 = trigger.getFireTimeAfter(time1);
        Date time3 = trigger.getFireTimeAfter(time2);
        Date time4 = trigger.getFireTimeAfter(time3);
        long l = time1.getTime() - (time4.getTime() - time2.getTime());
        times[0] = sd.format(new Date(l));
        times[1] = sd.format(new Date(Long.parseLong("1571729640000")));
        System.out.println(times[0]+"---"+times[1]);
    }
    //获取下次执行时间（getFireTimeAfter，也可以下下次...）
/*    public static long getNextTriggerTime(String cron){
      if(!CronExpression.isValidExpression(cron)){
        return 0;
      }
      CronTrigger trigger = TriggerBuilder.newTrigger().withIdentity("Caclulate Date").withSchedule(CronScheduleBuilder.cronSchedule(cron)).build();
      Date time0 = trigger.getStartTime();
      Date time1 = trigger.getFireTimeAfter(time0);
      return time1.getTime();
  }*/
}
