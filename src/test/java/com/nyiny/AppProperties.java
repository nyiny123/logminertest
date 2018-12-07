/*
 * Copyright (c) 2018 Navis LLC. All Rights Reserved.
 *
 */
package com.nyiny;

import com.sun.istack.internal.NotNull;
import com.sun.istack.internal.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This class simply reads properties
 */
class AppProperties {
    private static final String ORACLE_PROPERTIES = "oracle.properties";
    private Properties props;

    AppProperties(@Nullable String fileNameArg){
        InputStream input = null;
        try {
            String name = fileNameArg != null ? fileNameArg : ORACLE_PROPERTIES;
            input = AppProperties.class.getClassLoader().getResourceAsStream(name);
            props = new Properties();
            props.load(input);
        } catch (IOException io) {
            io.printStackTrace();
        } finally {
            if ( input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Nullable String getProperty(@NotNull String name){
        return props.getProperty(name);
    }
}
