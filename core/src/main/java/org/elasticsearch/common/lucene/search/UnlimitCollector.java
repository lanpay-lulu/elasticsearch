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

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;



/**
 *
 */

public class UnlimitCollector extends SimpleCollector {

    private Scorer scorer;
    private int docBase;
    private LeafReader reader;
    //private NumericDocValues docValues;
    private SortedNumericDocValues docValues;
    private List<Long> docIds = new ArrayList<>();
    private List<ScoreDoc> scoreDocs = new ArrayList<ScoreDoc>();
    private String IDField;
    private int hitNum;
    boolean getRowKey;

    public UnlimitCollector(String idfield, boolean rowkey) {
        this.IDField = idfield;
        getRowKey = rowkey;
        hitNum = 0;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
        docBase = context.docBase;
        reader = context.reader();
        docValues = reader.getSortedNumericDocValues(IDField);
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
        float score = 0; //scorer.score();
        ScoreDoc sd = new ScoreDoc(doc, score);

        if(getRowKey){
            docValues.setDocument(doc);
            Long val = docValues.valueAt(0);
        }

        hitNum += 1;
        if(hitNum % 1000000 == 0) {
            System.out.println("collect num = "+hitNum);
        }
        //docIds.add(val);
        //scoreDocs.add(sd);
        //System.out.println("age="+val);
    }

    //@Override
    public boolean needsScores() {
        return true;
    }
}
