/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.ltr.interleaving;

import java.util.Map;
import java.util.Set;
import org.apache.solr.ltr.LTRScoringQuery;
import org.apache.solr.ltr.LTRThreadModule;
import org.apache.solr.ltr.model.LTRScoringModel;

public class LTRInterleavingScoringQuery extends LTRScoringQuery {

  // Model was picked for this Docs
  private Set<Integer> pickedInterleavingDocIds;

  public LTRInterleavingScoringQuery(LTRScoringModel ltrScoringModel) {
    super(ltrScoringModel);
  }

  public LTRInterleavingScoringQuery(
      LTRScoringModel ltrScoringModel,
      Map<String, String[]> externalFeatureInfo,
      LTRThreadModule ltrThreadMgr) {
    super(ltrScoringModel, externalFeatureInfo, ltrThreadMgr);
  }

  public Set<Integer> getPickedInterleavingDocIds() {
    return pickedInterleavingDocIds;
  }

  public void setPickedInterleavingDocIds(Set<Integer> pickedInterleavingDocIds) {
    this.pickedInterleavingDocIds = pickedInterleavingDocIds;
  }

  @Override
  public LTRScoringQuery clone() {
    LTRInterleavingScoringQuery cloned = new LTRInterleavingScoringQuery(getScoringModel(),
            getExternalFeatureInfo(), getThreadModule());
    cloned.setOriginalQuery(getOriginalQuery());
    cloned.setFeatureLogger(getFeatureLogger());
    cloned.setRequest(getRequest());
    cloned.setPickedInterleavingDocIds(getPickedInterleavingDocIds());
    return cloned;
  }


  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }
  private boolean equalsTo(LTRInterleavingScoringQuery other) {
    boolean superSame = (super.equalsTo(other) && other.equalsTo((LTRScoringQuery) this));
    if (this.getPickedInterleavingDocIds() != null && other.getPickedInterleavingDocIds() != null) {
      return this.pickedInterleavingDocIds.equals(other.getPickedInterleavingDocIds());
    }
    else {
      return (this.pickedInterleavingDocIds == other.getPickedInterleavingDocIds());
    }
  }
}
