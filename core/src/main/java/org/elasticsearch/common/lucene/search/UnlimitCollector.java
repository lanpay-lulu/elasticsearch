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
package org.elasticsearch.common.lucene.search;


import org.apache.log4j.Logger;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;

import org.elasticsearch.search.RelSearchParam;
import org.relsearch.DocBatch;
import org.relsearch.DocInfo;
import org.relsearch.QueryResultClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


/**
 *
 */

public class UnlimitCollector extends SimpleCollector {

    public static final ExecutorService threadPool = Executors.newFixedThreadPool(24);
    //public static final ExecutorService threadPool = Executors.newFixedThreadPool();
    private static Logger logger = Logger.getLogger(UnlimitCollector.class);

    private Scorer scorer;
    private int docBase;
    private LeafReader reader;
    //private NumericDocValues docValues;
    private SortedNumericDocValues docValues;
    private List<DocInfo> docs = new ArrayList<>();
    //private List<ScoreDoc> scoreDocs = new ArrayList<ScoreDoc>();
    private String IdField;
    private int hitNum;
    boolean getRowKey;
    RelSearchParam relSearchParam;
    private QueryResultClient workerClient;
    private String indexName = "";
    private int shardId;
    private int localShardNum;
    private AtomicInteger totalBatchNum = new AtomicInteger(0);

    private boolean needSend = true;

    public UnlimitCollector(String idfield, RelSearchParam relSearchParam, QueryResultClient workerClient, String indexName, int shardId, int num) {
        this.IdField = idfield;
        getRowKey = (idfield!=null);
        hitNum = 0;
        this.relSearchParam = relSearchParam;
        this.workerClient = workerClient;
        this.indexName = indexName;
        this.shardId = shardId;
        this.localShardNum = num;
        if(relSearchParam.getSubqueryId()==-1){
            needSend = false;
        }
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
        docBase = context.docBase;
        reader = context.reader();
        try {
            docValues = reader.getSortedNumericDocValues(IdField);
        } catch (IOException e) {
            //e.printStackTrace();
            logger.error("doSetNextReader() error:"+e.getLocalizedMessage());
        }
        if(docValues == null) {
            logger.error("doSetNextReader() can not get docValues! IdField="+IdField);
            if(reader.getNumericDocValues(IdField) == null){
                logger.error("try getNumericDocValues() also failed.");
            }
        }


        //reader.
        //docValues = reader.getNumericDocValues(IDField);
        //docValues = reader.getNumericDocValues(IDField);
        /*System.out.println("doc values size = "+reader.getFieldInfos().size());
        for(int i=0; i<reader.getFieldInfos().size(); i++){
            System.out.println(reader.getFieldInfos().fieldInfo(i).getDocValuesType()+" "+reader.getFieldInfos().fieldInfo(i).name);
        }*/


    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        //super.setScorer(scorer);
        this.scorer = scorer;
    }

    @Override
    public void collect(int doc) throws IOException {
        //float score = 0; //scorer.score();
        //ScoreDoc sd = new ScoreDoc(doc, score);
        final int BatchSize = 10000;

        if(getRowKey){
            docValues.setDocument(doc);
            Long val = docValues.valueAt(0);
            docs.add(new DocInfo(val));
            if(docs.size() >= BatchSize && needSend) {
                sendDocs();
            }
        }

        hitNum += 1;
        if(hitNum % 10000 == 0) {
            //System.out.println("collect num = "+hitNum);
            logger.debug("wshlog current total hit="+hitNum+ ", docid="+doc);
        }
    }

    public void sendDocs() {
        if(docs.isEmpty()){
            return ;
        }
        logger.debug("sendDocs docnum="+docs.size());
        totalBatchNum.getAndIncrement();
        DocBatch batch = new DocBatch(
            relSearchParam.getRequestId(),
            relSearchParam.getSubqueryId(),
            indexName,
            shardId,
            localShardNum,
            docs);
        workerClient.send(batch);
        docs = new ArrayList<>();
    }

    //@Override
    public boolean needsScores() {
        return true;
    }

    public int hitNum() {
        return hitNum;
    }

    public int totalBatchNum() {
        return totalBatchNum.get();
    }

}
