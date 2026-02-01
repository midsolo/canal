package com.alibaba.otter.canal.filter.aviater;

import java.util.Map;

import org.apache.oro.text.regex.Perl5Matcher;

import com.alibaba.otter.canal.filter.PatternUtils;
import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.function.FunctionUtils;
import com.googlecode.aviator.runtime.type.AviatorBoolean;
import com.googlecode.aviator.runtime.type.AviatorObject;

/**
 * 对Aviator自定义的函数的扩展，内部使用到了oro中的Perl5Matcher来进行正则匹配
 */
public class RegexFunction extends AbstractFunction {

    public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
        String pattern = FunctionUtils.getStringValue(arg1, env);
        String text = FunctionUtils.getStringValue(arg2, env);
        Perl5Matcher matcher = new Perl5Matcher();
        boolean isMatch = matcher.matches(text, PatternUtils.getPattern(pattern));
        return AviatorBoolean.valueOf(isMatch);
    }

    public String getName() {
        return "regex";
    }

}
