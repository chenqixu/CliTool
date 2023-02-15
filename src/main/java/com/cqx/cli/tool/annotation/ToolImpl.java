package com.cqx.cli.tool.annotation;

import java.lang.annotation.*;

/**
 * ToolImpl
 *
 * @author chenqixu
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ToolImpl {
}
