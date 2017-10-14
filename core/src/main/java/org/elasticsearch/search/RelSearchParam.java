/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Created by lanpay on 2017/8/7.
 */
public final class RelSearchParam implements Writeable {



    private String requestId;
    private int subqueryId;

    public RelSearchParam(String requestId, int subqueryId) {
        this.requestId = requestId;
        this.subqueryId = subqueryId;
    }

    public RelSearchParam(StreamInput in) throws IOException {
        requestId = in.readString();
        subqueryId = in.readInt();
    }

    public String getRequestId() {
        return requestId;
    }

    public int getSubqueryId() {
        return subqueryId;
    }

    public static RelSearchParam parse(String s) {
        if(s == null){
            return null;
        }
        String[] args = s.split(",");
        if(args.length >= 2) {
            int subid = Integer.parseInt(args[1]);
            return new RelSearchParam(args[0], subid);
        }
        else {
            return new RelSearchParam(s, -1);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RelSearchParam rel = (RelSearchParam) o;
        return Objects.equals(requestId, rel.requestId) && (subqueryId==rel.subqueryId);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(requestId);
        out.writeInt(subqueryId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestId+subqueryId);
    }

    @Override
    public String toString() {
       return "RelSearchParam{" +
           "requestId="+requestId +
           ", subqueryId="+subqueryId +
           "}";
    }
}
