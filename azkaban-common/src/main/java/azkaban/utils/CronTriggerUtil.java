package azkaban.utils;

import org.apache.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.TriggerBuilder;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * 描述：
 *
 * @author zhilong.deng@hand-china.com
 * @version 0.01
 * @date 2019/10/22
 */
public class CronTriggerUtil {
    private static Logger logger = Logger.getLogger(CronTriggerUtil.class);
    private static final SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String[] getLastTriggerTime(String cron, Long now, Map<String, String> extraMap) {
        Long timeDiff = 0L;
        String way = "sub";
        if (extraMap.containsKey("timeDiff")) {
            String value = extraMap.get("timeDiff");
            if (value.startsWith("+")) {
                way = "add";
            }else if (value.startsWith("-")){
                way = "sub";
            } else{
                throw new RuntimeException("时间差格式错误!");
            }
            timeDiff = Long.parseLong(value.substring(1));
        }
        String[] times = new String[2];
        CronTrigger trigger = TriggerBuilder.newTrigger().withIdentity("Caclulate Date").withSchedule(CronScheduleBuilder.cronSchedule(cron)).build();
        Date time0 = trigger.getStartTime();
        Date time1 = trigger.getFireTimeAfter(time0);
        Date time2 = trigger.getFireTimeAfter(time1);
        Date time3 = trigger.getFireTimeAfter(time2);
        Date time4 = trigger.getFireTimeAfter(time3);
        long l = time1.getTime() - (time4.getTime() - time2.getTime());
        l = way == "add" ? l + timeDiff : l - timeDiff;
        now = way == "add" ? l + timeDiff : l - timeDiff;
        logger.info(String.format("------%s----%s-----%s-----%s----%s", time0, time1, time2, time3, time4));
        times[0] = sd.format(new Date(l));
        times[1] = sd.format(new Date(now));
        logger.info(String.format("------%s----%s", times[0], times[1]));
        return times;
    }
}
