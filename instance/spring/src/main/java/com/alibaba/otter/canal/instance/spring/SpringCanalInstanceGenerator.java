package com.alibaba.otter.canal.instance.spring;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.instance.core.CanalInstance;
import com.alibaba.otter.canal.instance.core.CanalInstanceGenerator;
import com.alibaba.otter.canal.parse.CanalEventParser;

/**
 * 提供了基于Spring配置方式的CanalInstanceWithSpring实现，即CanalInstance实例的创建，通过Spring配置文件来创建。
 */
public class SpringCanalInstanceGenerator implements CanalInstanceGenerator {
    private static final Logger logger = LoggerFactory.getLogger(SpringCanalInstanceGenerator.class);

    private String              springXml;
    private String              defaultName = "instance";
    private BeanFactory         beanFactory;

    @Override // CanalInstance生成流程
    public CanalInstance generate(String destination) {
        synchronized (CanalEventParser.class) {
            try {
                // ========1. 设置当前destination到系统属性========
                // Spring配置文件中会使用${canal.instance.destination}占位符
                System.setProperty("canal.instance.destination", destination);

                // ========2. 加载Spring配置文件========
                this.beanFactory = getBeanFactory(springXml);

                // ========3. 获取Bean========
                String beanName = destination;
                if (!beanFactory.containsBean(beanName)) {
                    beanName = defaultName; // 使用默认的"instance"bean
                }

                return (CanalInstance) beanFactory.getBean(beanName);
            } catch (Throwable e) {
                logger.error("generator instance failed.", e);
                throw new CanalException(e);
            } finally {
                // ========4. 清理系统属性========
                System.setProperty("canal.instance.destination", "");
            }
        }
    }

    private BeanFactory getBeanFactory(String springXml) {
        if (!StringUtils.startsWithIgnoreCase(springXml, "classpath:")) {
            springXml = "classpath:" + springXml;
        }
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext(springXml);
        return applicationContext;
    }

    public void setSpringXml(String springXml) {
        this.springXml = springXml;
    }

}
