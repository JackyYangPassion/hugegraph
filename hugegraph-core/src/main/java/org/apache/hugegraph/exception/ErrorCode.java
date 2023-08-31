/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.exception;

import org.apache.hugegraph.util.JsonUtil;

import java.util.*;

public enum ErrorCode {
    /*
     * Choose one of the following:
     *   use input message: errorCode.with(message)
     *   use ErrorCode.message: errorCode.format(args)
     *
     * attach list: for hubble to analyze
     * */

    // 00-xxx for common error
    SUCCEED("100-000", "Operation succeeded."),

    FAILED("100-001", "%s"),

    NULL_VALUE("100-002", "Invalid parameter , %s is null!"),

    INVALID_VALUE("100-003", "%s!"),

    MISSING_PARAM("100-004", "Invalid parameter, missing parameter '%s'"),

    NOT_EXISTS("100-005", "%s does not exist!"),

    EXISTS("100-006", "%s already exists!"),

    INVALID_PARAM("100-007", "The value '%s' of parameter '%s' is invalid!"),

    INVALID_VALUE_OUT_RANGE("100-008",
                            "Invalid parameter: the value of field '%s' " +
                            "is not in the valid range %s."),
    // 01-xxx for graphSpace error
    // 02-xxx for graphs error
    // 03-xxx for schema error
    UNDEFINED_PROPERTY_KEY("103-000", "Undefined property key: '%s'"),

    UNDEFINED_VERTEX_LABEL("103-001", "Undefined vertex label: '%s'"),

    UNDEFINED_EDGE_LABEL("103-002", "Undefined edge label: '%s'"),

    INVALID_PROP_VALUE("103-003", "Invalid data type of query value in %s, " +
                                  "expect %s for '%s', actual got %s"),

    // 04-xxx for vertex and edge error
    VERTEX_NOT_EXIST("104-001", "Vertex '%s' does not exist"),

    VERTEX_ID_NOT_EXIST("104-002", "The %s with id '%s' does not exist"),

    IDS_NOT_EXIST("104-003", "Not exist vertices with ids %s"),

    EDGE_INVALID_LINK("104-004", "Invalid source/target label %s,%s for edge " +
                                 "label %s"),

    LABEL_PROP_NOT_EXIST("104-005", "Not exist vertex with label '%s' and " +
                                    "properties '%s'"),

    // 05-xxx for oltp algorithms error
    REACH_CAPACITY("105-003", "Reach capacity '%s'"),

    REACH_CAPACITY_WITH_DEPTH("105-004", "Reach capacity '%s' while remaining depth '%s'"),

    EXCEED_CAPACITY_WHILE_FINDING("105-005", "Exceed capacity '%s' while finding %s"),

    INVALID_LIMIT("105-006", "Invalid limit %s, must be <= capacity(%s)"),

    PARAM_GREATER("105-007", "The %s must be >= %s, but got '%s' and '%s'"),

    PARAM_SMALLER("105-008", "The %s must be < %s"),

    DEPTH_OUT_RANGE("105-009", "The depth of request must be in (0, 5000], " +
                               "but got: %s"),

    VERTEX_PAIR_LENGTH_ERROR("105-010", "vertex pair length error"),

    SOURCE_TARGET_SAME("105-011", "The source & target vertex id can't be same"),
    ;

    private String code;
    private String message;
    private List<Object> attach;

    ErrorCode(String code, String message) {
        this.code = code;
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    public ErrorCode attach(Object... values) {
        attach = Arrays.asList(values);
        return this;
    }

    public Map<String, Object> build() {
        Map<String, Object> map = new HashMap<>();
        map.put("code", this.code);
        map.put("message", this.message);
        if (attach == null) {
            attach = new ArrayList<>();
        }
        map.put("attach", attach);
        return map;
    }

    public Map<String, Object> build(String message) {
        Map<String, Object> map = new HashMap<>();
        map.put("code", this.code);
        map.put("message", message);
        if (attach == null) {
            attach = new ArrayList<>();
        }
        map.put("attach", attach);
        return map;
    }

    public String format(Object... args) {
        this.attach(args);
        return JsonUtil.toJson(this.build(String.format(this.message, args)));
    }

    public String with(String message) {
        return JsonUtil.toJson(this.build(message));
    }
}
